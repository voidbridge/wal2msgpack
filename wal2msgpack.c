#include "postgres.h"
#include <replication/reorderbuffer.h>
#include <nodes/value.h>
#include <nodes/parsenodes.h>
#include <catalog/pg_class.h>
#include <utils/rel.h>
#include "catalog/pg_type.h"

#include "replication/logical.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "regex.h"
#include "msgpack.h"
#include "utils/numeric.h"

PG_MODULE_MAGIC;

/* These must be available to pg_dlsym() */
extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

/* define a time macro to convert TimestampTz into something more sane,
 * which in this case is microseconds since epoch
 */
#ifdef HAVE_INT64_TIMESTAMP
#define TIMESTAMPTZ_TO_USEC_SINCE_EPOCH(t)                                     \
  (t + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY *USECS_PER_SEC)) / 1000;
#else
#define TIMESTAMPTZ_TO_USEC_SINCE_EPOCH(t)                                     \
  (t + ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY));
#endif

typedef struct
{
    MemoryContext context;

	bool			new_transactional; /* you can't have write_in_chunks=true if using new_transactional=false */
    bool            write_in_chunks;        /* write in chunks? */
	
    uint64 filtered_entries;

    short     include;

    short     exclude;

    short     include_message_prefixes;

	char tables[8][255+1];

    regex_t     message_prefixes[8];

	/*  Used for the accumulation of the batch we want to write (filtered by table).  */
    msgpack_sbuffer* sbuf;
    msgpack_packer* pk;


} MsgPackDecodingData;



/* These must be available to pg_dlsym() */
static void pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, char is_init);
static void pg_decode_shutdown(LogicalDecodingContext *ctx);
static void pg_decode_begin_txn(LogicalDecodingContext *ctx,
                                ReorderBufferTXN *txn);
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
                                 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                             Relation relation, ReorderBufferChange *change);
static void
pg_decode_message(struct LogicalDecodingContext *ctx,
                  ReorderBufferTXN *txn,
                  XLogRecPtr message_lsn,
                  char transactional,
                  const char *prefix,
                  Size message_size,
                  const char *message);


static void write_event(LogicalDecodingContext *ctx, msgpack_sbuffer *sbuf);

void writeMessage(const char *prefix, Size sz, const char *message,msgpack_packer* pk);

static const int insert_change_type = 1;
static const int update_change_type = 2;
static const int delete_change_type = 3;
static const int message_change_type = 4;

static const int old_transactional_event = 9;
static const int none_transactional_event = 10;
static const int transactional_event = 11;

void _PG_init(void)
{
    /* other plugins can perform things here */
}

void _PG_output_plugin_init(OutputPluginCallbacks *cb)
{
    AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

    cb->startup_cb = pg_decode_startup;
    cb->begin_cb = pg_decode_begin_txn;
    cb->change_cb = pg_decode_change;
    cb->commit_cb = pg_decode_commit_txn;
    cb->shutdown_cb = pg_decode_shutdown;
    cb->message_cb = pg_decode_message;
    elog(DEBUG1, "initialize wal2msgpack");
}

/* Initialize this plugin */
static void
pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, char is_init)
{
    ListCell	*option;
    MsgPackDecodingData *data;

    data = palloc0(sizeof(MsgPackDecodingData));

    data->context = AllocSetContextCreate(TopMemoryContext,
                                          "wal2msgpack output context",
#if PG_VERSION_NUM >= 90600
                                          ALLOCSET_DEFAULT_SIZES
#else
                                          ALLOCSET_DEFAULT_MINSIZE,
                                          ALLOCSET_DEFAULT_INITSIZE,
                                          ALLOCSET_DEFAULT_MAXSIZE
#endif
    );

    elog(DEBUG1, "pg_decode_startup wal2msgpack");
    data->include = 0;
    data->exclude = 0;
    data->include_message_prefixes = 0;
    data->write_in_chunks = false;
    data->new_transactional = false;

    data->sbuf = msgpack_sbuffer_new();
    msgpack_sbuffer_init(data->sbuf);

    //serialize values into the buffer using msgpack_sbuffer_write callback function.
    data->pk = msgpack_packer_new(data->sbuf, msgpack_sbuffer_write);

    ctx->output_plugin_private = data;

    opt->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;

    foreach(option, ctx->output_plugin_options)
    {
        DefElem *elem = lfirst(option);

        Assert(elem->arg == NULL || IsA(elem->arg, String));

        if (strcmp(elem->defname, "include-tables") == 0)
        {
            if (data->exclude > 0)
            {
                elog(WARNING, "We are already excluding so can't have include-tables too");
            }
            else
            {
                char *token;
                data->include = 0;

                token = strtok(strVal(elem->arg), ",");
                while( token != NULL )
                {
                	strcpy(data->tables[data->include], token);
                    data->include++;

                    token = strtok(NULL, ",");
                }
            }

        }
        else if (strcmp(elem->defname, "exclude-tables") == 0)
        {
            if (data->include > 0)
            {
                elog(WARNING, "We are already including so can't have exclude-tables too");
            }
            else
            {
                char *token;
                data->exclude = 0;

                token = strtok(strVal(elem->arg), ",");

                while( token != NULL )
                {
                	strcpy(data->tables[data->exclude], token);
                    data->exclude++;

                    token = strtok(NULL, ",");
                }
            }

        }
        else if (strcmp(elem->defname, "include-message-prefixes") == 0)
        {
            char *token;
            data->include_message_prefixes = 0;

            token = strtok(strVal(elem->arg), ",");

            while( token != NULL )
            {
                int errorCode = regcomp(&data->message_prefixes[data->include_message_prefixes], token, 0);
                if(errorCode != 0)
                {
                    char msgbuf[100];
                    regerror(errorCode, &data->message_prefixes[data->include_message_prefixes], msgbuf, sizeof(msgbuf));
                    elog(WARNING, "Unable to compile [%s] regex error [%s]", token, msgbuf );
                }
                else
                {
                    data->include_message_prefixes++;
                }

                token = strtok(NULL, ",");
            }

        }
        else if (strcmp(elem->defname, "write-in-chunks") == 0)
        {
            if (elem->arg == NULL)
            {
                      elog(LOG, "write-in-chunks argument is null");
                      data->write_in_chunks = true;
            }
            else if (!parse_bool(strVal(elem->arg), &data->write_in_chunks))
                    ereport(ERROR,
                                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                 errmsg("could not parse value \"%s\" for parameter \"%s\"",
                                 strVal(elem->arg), elem->defname)));
        }
        else if (strcmp(elem->defname, "new-transactional") == 0)
        {
            if (elem->arg == NULL)
            {
                      elog(LOG, "new-transactional argument is null");
                      data->new_transactional = true;
            }
            else if (!parse_bool(strVal(elem->arg), &data->new_transactional))
                    ereport(ERROR,
                                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                 errmsg("could not parse value \"%s\" for parameter \"%s\"",
                                 strVal(elem->arg), elem->defname)));
        }
        else
        {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("option \"%s\" = \"%s\" is unknown",
                                   elem->defname,
                                   elem->arg ? strVal(elem->arg) : "(null)")));
        }
    }
    
    if (!data->new_transactional && data->write_in_chunks)
    {
    	data->write_in_chunks = false;
    	elog(WARNING, "Attempt to use write-in-chunks with old transactional batching (type 9) wal2msgpack");
    }
    
    elog(DEBUG1, "completed pg_decode_startup wal2msgpack");
}

/* cleanup this plugin's resources */
static void
pg_decode_shutdown(LogicalDecodingContext *ctx)
{
    MsgPackDecodingData *data = ctx->output_plugin_private;

    msgpack_packer_free(data->pk);
    msgpack_sbuffer_free(data->sbuf);
    /* cleanup our own resources via memory context reset */
    MemoryContextDelete(data->context);
}

/* BEGIN callback */
static void
pg_decode_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn) {
    MsgPackDecodingData *data = ctx->output_plugin_private;
    elog(DEBUG1, "pg_decode_begin_txn wal2msgpack");

    data->filtered_entries = 0;

    msgpack_sbuffer_clear(data->sbuf);

	if (data->new_transactional)
	{
	    msgpack_pack_int8(data->pk, transactional_event);
    	int64 commit = TIMESTAMPTZ_TO_USEC_SINCE_EPOCH(txn->commit_time);
	    msgpack_pack_int64(data->pk, commit);
    	msgpack_pack_uint64(data->pk, txn->end_lsn);
	}
}

static void
write_old_transactional(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
    msgpack_sbuffer sbuf;
    msgpack_packer pk;
    MsgPackDecodingData *data = ctx->output_plugin_private;

    /* msgpack::sbuffer is a simple buffer implementation. */
    msgpack_sbuffer_init(&sbuf);

    /* serialize values into the buffer using msgpack_sbuffer_write callback function. */
    msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

    msgpack_pack_int8(&pk, old_transactional_event);
    int64 commit = TIMESTAMPTZ_TO_USEC_SINCE_EPOCH(txn->commit_time);
    msgpack_pack_int64(&pk, commit);

    msgpack_pack_uint64(&pk, txn->end_lsn);

    msgpack_pack_array(&pk, data->filtered_entries);
    msgpack_pack_ext_body(&pk, data->sbuf->data, data->sbuf->size);

    write_event(ctx, &sbuf);
    msgpack_sbuffer_destroy(&sbuf);

}

static void write_new_event(LogicalDecodingContext *ctx)
{
    MsgPackDecodingData *data = ctx->output_plugin_private;

    write_event(ctx, data->sbuf);

    msgpack_sbuffer_clear(data->sbuf);
}

static void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                     XLogRecPtr commit_lsn)
{
    MsgPackDecodingData *data = ctx->output_plugin_private;

    if(!data->write_in_chunks && data->filtered_entries > 0)
    {
    	if (data->new_transactional)
    	{
		    write_new_event(ctx);
    	}
    	else
    	{
    		write_old_transactional(ctx, txn);
    	}
    }

    if (txn->has_catalog_changes)
        elog(DEBUG1, "txn has catalog changes: yes");
    else
        elog(DEBUG1, "txn has catalog changes: no");
    elog(DEBUG1, "filtered entries: %lu ; # of changes: %lu ; # of changes in memory: %lu", data->filtered_entries, txn->nentries, txn->nentries_mem);
    elog(DEBUG1, "# of subxacts: %d", txn->nsubtxns);

    /* clear the user data msg-pack buffer for next use */
    msgpack_sbuffer_clear(data->sbuf);
	
    data->filtered_entries = 0;
}

static void write_event(LogicalDecodingContext *ctx, msgpack_sbuffer *sbuf)
{
    OutputPluginPrepareWrite(ctx, true);
    appendBinaryStringInfo(ctx->out, sbuf->data, sbuf->size);
    OutputPluginWrite(ctx, true);
}


static void increment_actual_attrs(int* actual_attrs, MsgPackDecodingData	*data, Oid typid,
                                   Oid typoutput, char* name, Datum datum, char isnull)
{
    (*actual_attrs)++;
}

/* this doesn't seem to be available in the public api (unfortunate) */
static double numeric_to_double_no_overflow(Numeric num) {
    char *tmp;
    double val;
    char *endptr;

    tmp = DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(num)));

    /* unlike float8in, we ignore ERANGE from strtod */
    val = strtod(tmp, &endptr);
    if (*endptr != '\0') {
        /* shouldn't happen ... */
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        errmsg("invalid input syntax for type double precision: \"%s\"",
                               tmp)));
    }

    pfree(tmp);

    return val;
}

static void set_name_and_value(int* actual_attrs, MsgPackDecodingData	*data, Oid typid,
                               Oid typoutput, char* name, Datum datum, char isnull)
{
    Numeric num;
    char* output;

    msgpack_pack_str(data->pk, strlen(name));
    msgpack_pack_str_body(data->pk, name, strlen(name));

    if (isnull)
    {
        msgpack_pack_nil(data->pk);
    }
    else
    {
        switch (typid) {
            case BOOLOID:
                if(DatumGetBool(datum))
                {
                    msgpack_pack_true(data->pk);
                }
                else
                {
                    msgpack_pack_false(data->pk);
                }
                break;
            case INT2OID:
                msgpack_pack_int16(data->pk, DatumGetInt16(datum));
                break;
            case INT4OID:
                msgpack_pack_int32(data->pk, DatumGetInt32(datum));
                break;
            case INT8OID:
            case OIDOID:
                msgpack_pack_int64(data->pk, DatumGetInt64(datum));
                break;
            case FLOAT4OID:
                msgpack_pack_float(data->pk, DatumGetFloat4(datum));
                break;
            case FLOAT8OID:
                msgpack_pack_double(data->pk, DatumGetFloat8(datum));
                break;
            case NUMERICOID:
                num = DatumGetNumeric(datum);
                if (numeric_is_nan(num))
                {
                    msgpack_pack_nil(data->pk);
                }
                else
                {
                    msgpack_pack_double(data->pk, numeric_to_double_no_overflow(num));
                }
                break;
            case CHAROID:
            case VARCHAROID:
            case BPCHAROID:
            case TEXTOID:
            case JSONOID:
            case XMLOID:
            case UUIDOID:
                output = OidOutputFunctionCall(typoutput, datum);
                msgpack_pack_str(data->pk, strlen(output));
                msgpack_pack_str_body(data->pk, output, strlen(output));
                break;
            case TIMESTAMPOID:
                /*
                 * THIS FALLTHROUGH IS MAKING THE ASSUMPTION WE ARE ON UTC
                 */
            case TIMESTAMPTZOID:
            {
                int64 timestamp;
                timestamp = TIMESTAMPTZ_TO_USEC_SINCE_EPOCH(DatumGetTimestampTz(datum));
                msgpack_pack_int64(data->pk, timestamp);
                break;
            }
            case BYTEAOID:
            case POINTOID:
            default:
                elog(DEBUG1, "Unable to convert type %d setting value to nil", typid);
                msgpack_pack_nil(data->pk);
                break;
        }
    }

}

static void loop_attributes(LogicalDecodingContext *ctx, TupleDesc tupdesc, HeapTuple tuple, TupleDesc indexdesc,
                            bool replident,
                            void (*call_back) (int* actual_attrs, MsgPackDecodingData	*data, Oid typid, Oid typoutput, char* name, Datum datum, char isnull), int* actual_attrs)
{
    MsgPackDecodingData	*data;
    int natt;

    data = ctx->output_plugin_private;

    for (natt = 0; natt < tupdesc->natts; natt++)
    {
        Form_pg_attribute	attr;		/* the attribute itself */
        Oid					typid;		/* type of current attribute */
        HeapTuple			type_tuple;	/* information about a type */
        Oid					typoutput;	/* output function */
        char				typisvarlena;
        Datum				origval;	/* possibly toasted Datum */
        Datum				val;		/* definitely detoasted Datum */
        char				isnull;		/* column is null? */

        attr = TupleDescAttr(tupdesc, natt);

        elog(DEBUG1, "attribute \"%s\" (%d/%d)", NameStr(attr->attname), natt, tupdesc->natts);

        /* Do not print dropped or system columns */
        if (attr->attisdropped || attr->attnum < 0)
            continue;

        /* Search indexed columns in whole heap tuple */
        if (indexdesc != NULL)
        {
            int		j;
            bool	found_col = false;

            for (j = 0; j < indexdesc->natts; j++)
            {
                Form_pg_attribute	iattr;

                iattr = TupleDescAttr(indexdesc, j);

                if (strcmp(NameStr(attr->attname), NameStr(iattr->attname)) == 0)
                    found_col = true;

            }

            /* Print only indexed columns */
            if (!found_col)
                continue;
        }

        typid = attr->atttypid;

        /* Figure out type name */
        type_tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
        if (!HeapTupleIsValid(type_tuple))
            elog(ERROR, "cache lookup failed for type %u", typid);

        /* Get information needed for printing values of a type */
        getTypeOutputInfo(typid, &typoutput, &typisvarlena);

        /* Get Datum from tuple */
        origval = heap_getattr(tuple, natt + 1, tupdesc, &isnull);

        /* Skip nulls iif printing key/identity */
        if (isnull && replident)
            continue;

        /* XXX Unchanged TOAST Datum does not need to be output */
        if (!isnull && typisvarlena && VARATT_IS_EXTERNAL_ONDISK(origval))
        {
            elog(WARNING, "column \"%s\" has an unchanged TOAST", NameStr(attr->attname));
            continue;
        }

        /* Accumulate each column info */
        ReleaseSysCache(type_tuple);

        if (isnull)
        {
            val = 0;
        }
        else
        {
            if (typisvarlena)
                val = PointerGetDatum(PG_DETOAST_DATUM(origval));
            else
                val = origval;
        }

        call_back(actual_attrs, data, typid, typoutput, NameStr(attr->attname), val, isnull);
    }

}
/*
 * Accumulate tuple information and stores it at the end
 *
 * replident: is this tuple a replica identity?
 * hasreplident: does this tuple has an associated replica identity?
 */
static void
tuple_to_stringinfo(LogicalDecodingContext *ctx, TupleDesc tupdesc, HeapTuple tuple, TupleDesc indexdesc, bool replident, bool hasreplident)
{
    int                 actual_attrs;
    MsgPackDecodingData	*data;

    data = ctx->output_plugin_private;
    actual_attrs = 0;

    loop_attributes(ctx, tupdesc, tuple, indexdesc, replident, increment_actual_attrs, &actual_attrs);

    msgpack_pack_map(data->pk, actual_attrs);

    loop_attributes(ctx, tupdesc, tuple, indexdesc, replident, set_name_and_value, &actual_attrs);

}

/* Print columns information */
static void
columns_to_stringinfo(LogicalDecodingContext *ctx, TupleDesc tupdesc, HeapTuple tuple, bool hasreplident)
{
    tuple_to_stringinfo(ctx, tupdesc, tuple, NULL, false, hasreplident);
}

/* Print replica identity information */
static void
identity_to_stringinfo(LogicalDecodingContext *ctx, TupleDesc tupdesc, HeapTuple tuple, TupleDesc indexdesc)
{
    /* Last parameter does not matter */
    tuple_to_stringinfo(ctx, tupdesc, tuple, indexdesc, true, false);
}

static bool filter_table(const MsgPackDecodingData *data, const char *schemaandtable, int errorCode) {
    bool processTable;
    processTable = false;
    if (data->include > 0)
    {
        int n;
        for(n = 0; n < data->include;n++)
        {

            if (strcmp(data->tables[n], schemaandtable) == 0)
            {
                processTable = true;
                break;
            }
        }

    }
    else if (data->exclude > 0)
    {
        processTable = true;
        int n;
        for(n = 0;n < data->exclude;n++)
        {

            if (strcmp(data->tables[n], schemaandtable) == 0)
            {
                processTable = false;
                break;
            }
        }
    }

    return processTable;
}
/* Callback for individual changed tuples */
static void
pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                 Relation relation, ReorderBufferChange *change)
{
    MsgPackDecodingData *data;
    Form_pg_class class_form;

    char* schemaandtable;
    char *relNamespace;
    int errorCode;
    bool processTable;


    AssertVariableIsOfType(&pg_decode_change, LogicalDecodeChangeCB);

    elog(DEBUG1, "pg_decode_change wal2msgpack");
    data = ctx->output_plugin_private;
    class_form = RelationGetForm(relation);

    relNamespace = get_namespace_name(class_form->relnamespace);
    errorCode = asprintf(&schemaandtable, "%s.%s", relNamespace, NameStr(class_form->relname));
    if (errorCode == -1)
    {
        elog(ERROR, "Unable to concatenate schema and relationship error code %d, skip filter", errorCode );
        processTable = true;
    }
    else
    {
        processTable = filter_table(data, schemaandtable, errorCode);
    }

    free(schemaandtable);

    if(processTable)
    {
        MemoryContext old;
        Relation	indexrel;
        TupleDesc	indexdesc;
        TupleDesc	tupdesc;
        size_t relNamespaceLength;
        size_t relNameLength;

        tupdesc = RelationGetDescr(relation);

        /* Avoid leaking memory by using and resetting our own context */
        old = MemoryContextSwitchTo(data->context);

        /* Make sure rd_replidindex is set */
        RelationGetIndexList(relation);

        /* Sanity checks */
        switch (change->action)
        {
            case REORDER_BUFFER_CHANGE_INSERT:
                if (change->data.tp.newtuple == NULL)
                {
                    elog(WARNING, "no tuple data for INSERT in table \"%s\"", NameStr(class_form->relname));
                    MemoryContextSwitchTo(old);
                    MemoryContextReset(data->context);
                    return;
                }
                break;
            case REORDER_BUFFER_CHANGE_UPDATE:
                /*
                 * Bail out iif:
                 * (i) doesn't have a pk and replica identity is not full;
                 * (ii) replica identity is nothing.
                 */
                if (!OidIsValid(relation->rd_replidindex) && relation->rd_rel->relreplident != REPLICA_IDENTITY_FULL)
                {
                    /* FIXME this sentence is imprecise */
                    elog(WARNING, "table \"%s\" without primary key or replica identity is nothing", NameStr(class_form->relname));
                    MemoryContextSwitchTo(old);
                    MemoryContextReset(data->context);
                    return;
                }

                if (change->data.tp.newtuple == NULL)
                {
                    elog(WARNING, "no tuple data for UPDATE in table \"%s\"", NameStr(class_form->relname));
                    MemoryContextSwitchTo(old);
                    MemoryContextReset(data->context);
                    return;
                }
                break;
            case REORDER_BUFFER_CHANGE_DELETE:
                /*
                 * Bail out iif:
                 * (i) doesn't have a pk and replica identity is not full;
                 * (ii) replica identity is nothing.
                 */
                if (!OidIsValid(relation->rd_replidindex) && relation->rd_rel->relreplident != REPLICA_IDENTITY_FULL)
                {
                    /* FIXME this sentence is imprecise */
                    elog(WARNING, "table \"%s\" without primary key or replica identity is nothing", NameStr(class_form->relname));
                    MemoryContextSwitchTo(old);
                    MemoryContextReset(data->context);
                    return;
                }

                if (change->data.tp.oldtuple == NULL)
                {
                    elog(WARNING, "no tuple data for DELETE in table \"%s\"", NameStr(class_form->relname));
                    MemoryContextSwitchTo(old);
                    MemoryContextReset(data->context);
                    return;
                }
                break;
            default:
                Assert(false);
        }

        /* Print change kind */
        switch (change->action)
        {
            case REORDER_BUFFER_CHANGE_INSERT:
                msgpack_pack_int8(data->pk, insert_change_type);
                break;
            case REORDER_BUFFER_CHANGE_UPDATE:
                msgpack_pack_int8(data->pk, update_change_type);
                break;
            case REORDER_BUFFER_CHANGE_DELETE:
                msgpack_pack_int8(data->pk, delete_change_type);
                break;
            default:
                Assert(false);
        }

        relNamespaceLength = strlen(relNamespace);
        msgpack_pack_str(data->pk, relNamespaceLength);
        msgpack_pack_str_body(data->pk, relNamespace, relNamespaceLength);

        relNameLength = strlen(NameStr(class_form->relname));
        msgpack_pack_str(data->pk, relNameLength);
        msgpack_pack_str_body(data->pk, NameStr(class_form->relname), relNameLength);

        switch (change->action)
        {
            case REORDER_BUFFER_CHANGE_INSERT:
                /* Print the new tuple */
                columns_to_stringinfo(ctx, tupdesc, &change->data.tp.newtuple->tuple, false);
                break;
            case REORDER_BUFFER_CHANGE_UPDATE:
                /* Print the new tuple */
                columns_to_stringinfo(ctx, tupdesc, &change->data.tp.newtuple->tuple, true);

                /*
                 * The old tuple is available when:
                 * (i) pk changes;
                 * (ii) replica identity is full;
                 * (iii) replica identity is index and indexed column changes.
                 *
                 * FIXME if old tuple is not available we must get only the indexed
                 * columns (the whole tuple is printed).
                 */
                if (change->data.tp.oldtuple == NULL)
                {
                    elog(DEBUG1, "old tuple is null");

                    indexrel = RelationIdGetRelation(relation->rd_replidindex);
                    if (indexrel != NULL)
                    {
                        indexdesc = RelationGetDescr(indexrel);
                        identity_to_stringinfo(ctx, tupdesc, &change->data.tp.newtuple->tuple, indexdesc);
                        RelationClose(indexrel);
                    }
                    else
                    {
                        identity_to_stringinfo(ctx, tupdesc, &change->data.tp.newtuple->tuple, NULL);
                    }
                }
                else
                {
                    elog(DEBUG1, "old tuple is not null");
                    identity_to_stringinfo(ctx, tupdesc, &change->data.tp.oldtuple->tuple, NULL);
                }
                break;
            case REORDER_BUFFER_CHANGE_DELETE:
                /* Print the replica identity */
                indexrel = RelationIdGetRelation(relation->rd_replidindex);
                if (indexrel != NULL)
                {
                    indexdesc = RelationGetDescr(indexrel);
                    identity_to_stringinfo(ctx, tupdesc, &change->data.tp.oldtuple->tuple, indexdesc);
                    RelationClose(indexrel);
                }
                else
                {
                    identity_to_stringinfo(ctx, tupdesc, &change->data.tp.oldtuple->tuple, NULL);
                }

                if (change->data.tp.oldtuple == NULL)
                    elog(DEBUG1, "old tuple is null");
                else
                    elog(DEBUG1, "old tuple is not null");
                break;
            default:
                Assert(false);
        }

        data->filtered_entries++;

    	if(data->write_in_chunks)
    	{
	        write_new_event(ctx);
    	}
	
        MemoryContextSwitchTo(old);
        MemoryContextReset(data->context);

    }
}

static void
pg_decode_message(struct LogicalDecodingContext *ctx,
                  ReorderBufferTXN *txn,
                  XLogRecPtr message_lsn,
                  char transactional,
                  const char *prefix,
                  Size message_size,
                  const char *message)
{
    MsgPackDecodingData *data;
    bool matched = false;
    int errorCode;

    data = ctx->output_plugin_private;

    if (data->include_message_prefixes > 0)
    {
        int n;
        for(n = 0; n < data->include_message_prefixes;n++)
        {

            errorCode = regexec(&data->message_prefixes[n], prefix, 0, NULL, 0);
            if (errorCode == 0)
            {
                matched = true;
                break;
            }
            else if (errorCode != REG_NOMATCH)
            {
                char msgbuf[100];
                regerror(errorCode, &data->message_prefixes[n], msgbuf, sizeof(msgbuf));
                elog(WARNING, "Unable to execute regex [%d] on prefix [%s] error [%s]", n, prefix, msgbuf );
            }
        }

    }

    if(matched)
    {
        MemoryContext old;
        /* Avoid leaking memory by using and resetting our own context */
        old = MemoryContextSwitchTo(data->context);

        if (transactional)
        {
            elog(DEBUG1, "writing transactional pg_decode_message");
            msgpack_pack_int8(data->pk, message_change_type);
            writeMessage(prefix, message_size, message, data->pk);
            data->filtered_entries++;
    		if(data->write_in_chunks)
    		{
	        	write_new_event(ctx);
    		}
        }
        else
        {
            elog(DEBUG1, "writing none transactional pg_decode_message");
            msgpack_sbuffer sbuf;
            msgpack_packer pk;

            /* msgpack::sbuffer is a simple buffer implementation. */
            msgpack_sbuffer_init(&sbuf);

            /* serialize values into the buffer using msgpack_sbuffer_write callback function. */
            msgpack_packer_init(&pk, &sbuf, msgpack_sbuffer_write);

            msgpack_pack_int8(&pk, none_transactional_event);

            writeMessage(prefix, message_size, message, &pk);

    		OutputPluginPrepareWrite(ctx, true);
    		appendBinaryStringInfo(ctx->out, sbuf.data, sbuf.size);
    		OutputPluginWrite(ctx, true);
            
            msgpack_sbuffer_destroy(&sbuf);
        }

        MemoryContextSwitchTo(old);
        MemoryContextReset(data->context);
    }
}

void writeMessage(const char *prefix, Size sz, const char *message,msgpack_packer* pk)
{
    size_t stringLength;
    stringLength = strlen(prefix);
    msgpack_pack_str(pk, stringLength);
    msgpack_pack_str_body(pk, prefix, stringLength);

    msgpack_pack_str(pk, sz);
    msgpack_pack_str_body(pk, message, sz);
}
