#include "postgres.h"
#include <replication/reorderbuffer.h>
#include <catalog/pg_class.h>
#include <sys/time.h>

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

    uint64		nentries;			/* txn->nentries */

    bool        first_entry;

    short     include;

    short     exclude;

    regex_t       tables[8];

    msgpack_sbuffer* sbuf;
    msgpack_packer* pk;
} MsgPackDecodingData;


/* These must be available to pg_dlsym() */
static void pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void pg_decode_shutdown(LogicalDecodingContext *ctx);
static void pg_decode_begin_txn(LogicalDecodingContext *ctx,
                                ReorderBufferTXN *txn);
static void pg_decode_commit_txn(LogicalDecodingContext *ctx,
                                 ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pg_decode_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                             Relation relation, ReorderBufferChange *change);
//static bool pg_decode_filter(LogicalDecodingContext *ctx, RepOriginId origin_id);

#if	PG_VERSION_NUM >= 90600
static void pg_decode_message(LogicalDecodingContext *ctx,
                              ReorderBufferTXN *txn, XLogRecPtr lsn,
                              bool transactional, const char *prefix,
                              Size content_size, const char *content);
#endif

static void write_event(LogicalDecodingContext *ctx, MsgPackDecodingData *data);

static const int begin_transaction_event = 0;
static const int commit_transaction_event = 1;
static const int insert_event = 5;
static const int update_event = 6;
static const int delete_event = 7;

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
 //   cb->filter_by_origin_cb = pg_decode_filter;
    cb->shutdown_cb = pg_decode_shutdown;
#if	PG_VERSION_NUM >= 90600
 //   cb->message_cb = pg_decode_message;
#endif
}

/* Initialize this plugin */
static void
pg_decode_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
    ListCell	*option;
    MsgPackDecodingData *data;

    data = palloc0(sizeof(MsgPackDecodingData));
#if PG_VERSION_NUM >= 90600
/*    data->context = AllocSetContextCreate(TopMemoryContext,
                                          "wal2msgpack output context",
                                          ALLOCSET_DEFAULT_SIZES
    );*/
#else
    data->context = AllocSetContextCreate(TopMemoryContext,
                                          "wal2msgpack output context",
                                        ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE
    );
#endif

    data->nentries = 0;
    data->first_entry = true;
    data->include = 0;
    data->exclude = 0;

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
                    int errorCode = regcomp(&data->tables[data->include], token, 0);
                    if(errorCode != 0)
                    {
                        char msgbuf[100];
                        regerror(errorCode, &data->tables[data->include], msgbuf, sizeof(msgbuf));
                        elog(WARNING, "Unable to compile [%s] regex error [%s]", token, msgbuf );
                    }
                    else
                    {
                        data->include++;
                    }

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
                    int errorCode = regcomp(&data->tables[data->exclude], token, 0);
                    if(errorCode != 0)
                    {
                        char msgbuf[100];
                        regerror(errorCode, &data->tables[data->include], msgbuf, sizeof(msgbuf));
                        elog(WARNING, "Unable to compile [%s] regex error [%s]", token, msgbuf );
                    }
                    else
                    {
                        data->exclude++;
                    }

                    token = strtok(NULL, ",");
                }
            }

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

    data->nentries = txn->nentries;
    data->first_entry = true;
}

static void
pg_decode_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                     XLogRecPtr commit_lsn)
{
    MsgPackDecodingData *data = ctx->output_plugin_private;

    if (txn->has_catalog_changes)
        elog(DEBUG1, "txn has catalog changes: yes");
    else
        elog(DEBUG1, "txn has catalog changes: no");
    elog(DEBUG1, "my change counter: %lu ; # of changes: %lu ; # of changes in memory: %lu", data->nentries, txn->nentries, txn->nentries_mem);
    elog(DEBUG1, "# of subxacts: %d", txn->nsubtxns);
}

static void write_event(LogicalDecodingContext *ctx, MsgPackDecodingData *data)
{
    OutputPluginPrepareWrite(ctx, true);
    appendBinaryStringInfo(ctx->out, data->sbuf->data, data->sbuf->size);
    OutputPluginWrite(ctx, true);
}


static void increment_actual_attrs(int* actual_attrs, MsgPackDecodingData	*data, Oid typid,
                                   Oid typoutput, char* name, Datum datum, bool isnull)
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
                               Oid typoutput, char* name, Datum datum, bool isnull)
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
                int64 commit;
                commit = TIMESTAMPTZ_TO_USEC_SINCE_EPOCH(DatumGetTimestampTz(datum));
                msgpack_pack_int64(data->pk, commit);
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
                            void (*call_back) (int* actual_attrs, MsgPackDecodingData	*data, Oid typid, Oid typoutput, char* name, Datum datum, bool isnull), int* actual_attrs)
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
        bool				typisvarlena;
        Datum				origval;	/* possibly toasted Datum */
        Datum				val;		/* definitely detoasted Datum */
        bool				isnull;		/* column is null? */

        /*
         * Commit d34a74dd064af959acd9040446925d9d53dff15b introduced
         * TupleDescAttr() in back branches. If the version supports
         * this macro, use it. Version 10 and later already support it.
         */
#if (PG_VERSION_NUM >= 90600 && PG_VERSION_NUM < 90605) || (PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90509) || (PG_VERSION_NUM >= 90400 && PG_VERSION_NUM < 90414)
        attr = tupdesc->attrs[natt];
#else
        attr = TupleDescAttr(tupdesc, natt);
#endif

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

                /* See explanation a few lines above. */
#if (PG_VERSION_NUM >= 90600 && PG_VERSION_NUM < 90605) || (PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90509) || (PG_VERSION_NUM >= 90400 && PG_VERSION_NUM < 90414)
                iattr = indexdesc->attrs[j];
#else
                iattr = TupleDescAttr(indexdesc, j);
#endif

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
    processTable = true;
    if (data->include > 0)
    {
        bool matched = false;
        int n;
        for(n = 0; n < data->include;n++)
        {

            errorCode = regexec(&data->tables[n], schemaandtable, 0, NULL, 0);
            if (errorCode == 0)
            {
                matched = true;
                break;
            }
            else if (errorCode != REG_NOMATCH)
            {
                char msgbuf[100];
                regerror(errorCode, &data->tables[n], msgbuf, sizeof(msgbuf));
                elog(WARNING, "Unable to execute regex [%d] on relationship [%s] error [%s]", n, schemaandtable, msgbuf );
            }
        }

        if(!matched)
        {
            processTable = false;
        }
    }
    else if (data->exclude > 0)
    {
        bool matched = false;
        int n;
        for(n = 0;n < data->exclude;n++)
        {
            errorCode = regexec(&data->tables[n], schemaandtable, 0, NULL, 0);
            if (errorCode == 0)
            {
                matched = true;
                break;
            }
            else if (errorCode != REG_NOMATCH)
            {
                char msgbuf[100];
                regerror(errorCode, &data->tables[n], msgbuf, sizeof(msgbuf));
                elog(WARNING, "Unable to execute regex [%d] on relationship [%s] error [%s]", n, schemaandtable, msgbuf );
            }
        }

        if(matched)
        {
            processTable = false;
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
        int64 commit;

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

        /* Change counter */
        data->nentries = txn->nentries;
        msgpack_sbuffer_clear(data->sbuf);

        /* Print change kind */
        switch (change->action)
        {
            case REORDER_BUFFER_CHANGE_INSERT:
                msgpack_pack_int8(data->pk, insert_event);
                break;
            case REORDER_BUFFER_CHANGE_UPDATE:
                msgpack_pack_int8(data->pk, update_event);
                break;
            case REORDER_BUFFER_CHANGE_DELETE:
                msgpack_pack_int8(data->pk, delete_event);
                break;
            default:
                Assert(false);
        }

        commit = TIMESTAMPTZ_TO_USEC_SINCE_EPOCH(txn->commit_time);
        msgpack_pack_int64(data->pk, commit);

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

        write_event(ctx, data);

        MemoryContextSwitchTo(old);
        MemoryContextReset(data->context);

        data->first_entry = false;
    }
}

/*
static bool pg_decode_filter(LogicalDecodingContext *ctx, RepOriginId origin_id)
{

}
*/

#if	PG_VERSION_NUM >= 90600
/* Callback for generic logical decoding messages */
/*static void
pg_decode_message(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
                  XLogRecPtr lsn, bool transactional, const char *prefix, Size
                  content_size, const char *content)
{
}*/
#endif
