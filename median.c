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
#include "utils/syscache.h"
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
 * Source modified from the patch submitted by Pavel Stehule to postgres in Aug 2010.
 */
PG_FUNCTION_INFO_V1(median_transfn);

PG_FUNCTION_INFO_V1(median_finalfn);

#define INMEMORYCAPACITY 10000
typedef struct{
    void* tuple;
    Datum datum;
}RawData;

typedef struct {
    int nelems;        /* number of valid entries */
    Tuplesortstate *sortstate;
    Oid valtype;
    bool inmemory;
    RawData* rawData;
} StatAggState;

void initialiseSortState(StatAggState * aggState, FunctionCallInfo fcinfo);
void initialiseSortState(StatAggState * , FunctionCallInfo );
StatAggState *initialiseState(FunctionCallInfo);

void initialiseSortState(StatAggState * aggState, FunctionCallInfo fcinfo){

    Oid collation;
    Oid sortop, eqop,gtop;
    Type t;
    MemoryContext oldctx;
    MemoryContext aggcontext;
    if (!AggCheckCallContext(fcinfo, &aggcontext)) {
        /* cannot be called directly because of internal-type argument */
        elog(ERROR, "string_agg_transfn called in non-aggregate context");
    }

    oldctx = MemoryContextSwitchTo(aggcontext);
    get_sort_group_operators(aggState->valtype, true, true, true, &sortop, &eqop, &gtop, NULL);

    t=typeidType(aggState->valtype);
    collation=typeTypeCollation(t);
    ReleaseSysCache(t);
    aggState->sortstate = tuplesort_begin_datum(aggState->valtype, sortop, collation,SORTBY_NULLS_DEFAULT, work_mem, false);
    MemoryContextSwitchTo(oldctx);
 }

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */

 StatAggState * initialiseState(FunctionCallInfo fcinfo){
    MemoryContext oldctx;
    MemoryContext aggcontext;
    Oid valtype;

    StatAggState *aggstate;
    if (!AggCheckCallContext(fcinfo, &aggcontext)) {
        /* cannot be called directly because of internal-type argument */
        elog(ERROR, "string_agg_transfn called in non-aggregate context");
    }

    oldctx = MemoryContextSwitchTo(aggcontext);

    aggstate = (StatAggState *) palloc(sizeof(StatAggState));
    aggstate->nelems = 0;
    aggstate->inmemory=false;

    valtype = get_fn_expr_argtype(fcinfo->flinfo, 1);
    aggstate->valtype=valtype;
    aggstate->sortstate=NULL;
    aggstate->rawData =palloc(INMEMORYCAPACITY* sizeof(RawData));

    MemoryContextSwitchTo(oldctx);

    return aggstate;
}


Datum
median_transfn(PG_FUNCTION_ARGS) {

    StatAggState *aggstate;

    aggstate = PG_ARGISNULL(0) ? NULL : (StatAggState *) PG_GETARG_POINTER(0);

    if (!PG_ARGISNULL(1)) {
        if (aggstate == NULL){
            aggstate=initialiseState(fcinfo);
            initialiseSortState(aggstate,fcinfo);
        }
        if(aggstate->inmemory){
            if(aggstate->nelems==INMEMORYCAPACITY){
                aggstate->inmemory=false;
                int iterator=0;
                for(iterator=0;iterator<aggstate->nelems;iterator++){
                    tuplesort_putdatum(aggstate->sortstate, aggstate->rawData[iterator].datum, false);
                }
            }else{
                aggstate->rawData[aggstate->nelems].datum = PG_GETARG_DATUM(1);
                if(!aggstate->sortstate->tuples){
                    aggstate->rawData[aggstate->nelems].tuple=DatumGetPointer(aggstate->rawData[aggstate->nelems].datum);
                }else{
                    aggstate->rawData[aggstate->nelems].tuple=NULL;
                }
            }
//            tuplesort_putdatum(aggstate->sortstate, PG_GETARG_DATUM(1), false);
            aggstate->nelems++;
        }else{
            tuplesort_putdatum(aggstate->sortstate, PG_GETARG_DATUM(1), false);
            aggstate->nelems++;
        }
    }

    PG_RETURN_POINTER(aggstate);
}

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */

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
        if(aggstate->inmemory){

        } else {
            hidx = aggstate->nelems / 2 + 1;
            lidx = (aggstate->nelems + 1) / 2;

            tuplesort_performsort(aggstate->sortstate);

            while (tuplesort_getdatum(aggstate->sortstate,
                                      true,
                                      &value, &isNull, NULL)) {
                if (i++ == lidx) {
                    if (aggstate->valtype != CSTRINGOID && aggstate->valtype != VARCHAROID) {
                        result = datumCopy(value, true, -1);
                    } else {
                        result = datumCopy(value, false, 0);
                    }

                    if (lidx != hidx) {
                        tuplesort_getdatum(aggstate->sortstate,
                                           true,
                                           &value, &isNull, NULL);
                        switch (aggstate->valtype) {
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
        }
        tuplesort_end(aggstate->sortstate);

        PG_RETURN_DATUM(result);
    } else
        PG_RETURN_NULL();
}



