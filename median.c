#include <postgres.h>
#include <fmgr.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "parser/parse_coerce.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/tuplesort.h"
#include "utils/datum.h"
#include "parser/parse_type.h"
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(median_transfn);

PG_FUNCTION_INFO_V1(median_finalfn);

typedef struct {
    int nelems;        /* number of valid entries */
    Tuplesortstate *sortstate;
    FmgrInfo cast_func_finfo;
    Oid valtype;
    int p;        /* nth for percentille */
} StatAggState;

static StatAggState *
makeStatAggState(FunctionCallInfo fcinfo) {
    MemoryContext oldctx;
    MemoryContext aggcontext;
    StatAggState *aggstate;
    Oid sortop, castfunc;
    Oid valtype;
//    Oid sortCollation;
    CoercionPathType pathtype;

    if (!AggCheckCallContext(fcinfo, &aggcontext)) {
        /* cannot be called directly because of internal-type argument */
        elog(ERROR, "string_agg_transfn called in non-aggregate context");
    }

    oldctx = MemoryContextSwitchTo(aggcontext);

    aggstate = (StatAggState *) palloc(sizeof(StatAggState));
    aggstate->nelems = 0;

    valtype = get_fn_expr_argtype(fcinfo->flinfo, 1);
    aggstate->valtype=valtype;
    get_sort_group_operators(valtype, true, false, false, &sortop, NULL, NULL, NULL);

    aggstate->sortstate = tuplesort_begin_datum(valtype, sortop, InvalidOid,SORTBY_NULLS_DEFAULT, work_mem, false);

    MemoryContextSwitchTo(oldctx);

    if (valtype != FLOAT8OID) {
        /* find a cast function */

        pathtype = find_coercion_pathway(FLOAT8OID, valtype,
                                         COERCION_EXPLICIT,
                                         &castfunc);
        if (pathtype == COERCION_PATH_FUNC) {
            Assert(OidIsValid(castfunc));
            fmgr_info_cxt(castfunc, &aggstate->cast_func_finfo,
                          aggcontext);
        } else if (pathtype == COERCION_PATH_RELABELTYPE) {
            aggstate->cast_func_finfo.fn_oid = InvalidOid;
        } else
            elog(ERROR, "no conversion function from %s %s",
                 format_type_be(valtype),
                 format_type_be(FLOAT8OID));
    }

    return aggstate;
}

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS) {
//    MemoryContext agg_context;

//    if (!AggCheckCallContext(fcinfo, &agg_context))
//        elog(ERROR, "median_transfn called in non-aggregate context");

//    PG_RETURN_NULL();
    StatAggState *aggstate;

    aggstate = PG_ARGISNULL(0) ? NULL : (StatAggState *) PG_GETARG_POINTER(0);

    if (!PG_ARGISNULL(1)) {
        if (aggstate == NULL){
            MemoryContext oldctx;
            MemoryContext aggcontext;
            Oid sortop, castfunc;
            Oid valtype;
            CoercionPathType pathtype;

            if (!AggCheckCallContext(fcinfo, &aggcontext)) {
                /* cannot be called directly because of internal-type argument */
                elog(ERROR, "string_agg_transfn called in non-aggregate context");
            }

            oldctx = MemoryContextSwitchTo(aggcontext);

            aggstate = (StatAggState *) palloc(sizeof(StatAggState));
            aggstate->nelems = 0;

            valtype = get_fn_expr_argtype(fcinfo->flinfo, 1);
            aggstate->valtype=valtype;
            get_sort_group_operators(valtype, true, false, false, &sortop, NULL, NULL, NULL);

            aggstate->sortstate = tuplesort_begin_datum(valtype, sortop, typeTypeCollation(typeidType(valtype)),SORTBY_NULLS_DEFAULT, work_mem, false);

            MemoryContextSwitchTo(oldctx);
        }

        tuplesort_putdatum(aggstate->sortstate, PG_GETARG_DATUM(1), false);
        aggstate->nelems++;
    }

    PG_RETURN_POINTER(aggstate);
}



Datum
median_finalfn(PG_FUNCTION_ARGS) {
    StatAggState *aggstate;

    Assert(AggCheckCallContext(fcinfo, NULL));

    aggstate = PG_ARGISNULL(0) ? NULL : (StatAggState *) PG_GETARG_POINTER(0);

    if (aggstate != NULL) {
        int lidx;
        int hidx;
        Datum value;
        bool isNull;
        int i = 1;
        Datum result=0;

        hidx = aggstate->nelems / 2 + 1;
        lidx = (aggstate->nelems + 1) / 2;

        tuplesort_performsort(aggstate->sortstate);

        while (tuplesort_getdatum(aggstate->sortstate,
                                  true,
                                  &value, &isNull,NULL)) {
            if (i++ == lidx) {
                if (aggstate->valtype != CSTRINGOID && aggstate->valtype != VARCHAROID) {
                    result = datumCopy(value, true, -1);
                }else{
                    result=datumCopy(value,false,0);
                }

                if (lidx != hidx) {
                    tuplesort_getdatum(aggstate->sortstate,
                                       true,
                                       &value, &isNull,NULL);
                    switch (aggstate->valtype){
                        case (INT8OID):
                            result = (DatumGetInt64(result) + DatumGetInt64(value)) / 2.0;
                            break;
                        case (INT4OID):
                            result = (DatumGetInt32(result) + DatumGetInt32(value)) / 2.0;
                            break;
                        case (INT2OID):
                            result = (DatumGetInt32(result) + DatumGetInt32(value)) / 2.0;
                            break;
                        case (FLOAT4OID):
                            result = (DatumGetFloat4(result) + DatumGetFloat4(value)) / 2.0;
                            break;
                        case (FLOAT8OID):
                            result = (DatumGetFloat8(result) + DatumGetFloat8(value)) / 2.0;
                            break;

                        default:
                            break;
                    }

                }
                break;
            }
        }

        tuplesort_end(aggstate->sortstate);

        PG_RETURN_DATUM(result);
    } else
        PG_RETURN_NULL();
}

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
//Datum
//median_finalfn(PG_FUNCTION_ARGS)
//{
//    MemoryContext agg_context;
//
//    if (!AggCheckCallContext(fcinfo, &agg_context))
//        elog(ERROR, "median_finalfn called in non-aggregate context");
//
//    PG_RETURN_NULL();
//}
