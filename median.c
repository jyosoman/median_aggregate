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
#include "utils/lsyscache.h"
#include "utils/sortsupport.h"
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/*
 * Source modified from the patch submitted by Pavel Stehule to postgres in Aug 2010.
 */

/*
 * Registering functions to be visible to Postgres
 */
PG_FUNCTION_INFO_V1(median_transfn);

PG_FUNCTION_INFO_V1(median_finalfn);

/*
 * Capacity of the inmemory buffer to be used.
 *
 */

#define INMEMORYCAPACITY 10000

typedef struct
{
	void	   *tuple;
	Datum		datum;
} RawData;

typedef struct
{
	int			nelems;			/* number of valid entries */
	Tuplesortstate *sortstate;
	Oid			valtype;		/* Type of data */
	bool		inmemory;
	RawData    *rawData;		/* Store for inmemory processing */
	bool		isTuple;		/* To check if the data is a tuple or not */
} StatAggState;

/* Forward declarations*/
void		sort_state_initialise(StatAggState *aggState, FunctionCallInfo fcinfo);
StatAggState *state_initialise(FunctionCallInfo);
int partition(RawData *, int, int);
Datum quick_select(RawData *, int, int, int);

/*
 * Helper function for quick_select
 * Splits data into two chunks, one less than pivot, other greater than
 * TODO: Need to find comparator functions for operators. Currently only numerics are supported.
 */

int
partition(RawData *input, int left_index, int right_index)
{
	Datum		pivot = input[right_index].datum;

	while (left_index < right_index)
	{
		while (input[left_index].datum < pivot)
		{
			left_index++;
		}

		while (input[right_index].datum > pivot)
		{
			right_index--;
		}

		if (input[left_index].datum == input[right_index].datum)
		{
			left_index++;
		}
		else if (left_index < right_index)
		{
			Datum		tmp = input[left_index].datum;

			input[left_index].datum = input[right_index].datum;
			input[right_index].datum = tmp;
		}
	}

	return right_index;
}

/*
 *
 *
 */
Datum
quick_select(RawData *input, int left_index, int right_index, int center)
{
	int			partitionIndex,
				length;

	if (left_index == right_index)
	{
		return input[left_index].datum;
	}

	partitionIndex = partition(input, left_index, right_index);

	length = partitionIndex - left_index + 1;

	if (length == center)
	{
		return input[partitionIndex].datum;
	}
	else if (center < length)
	{
		return quick_select(input, left_index, partitionIndex - 1, center);
	}
	else
	{
		return quick_select(input, partitionIndex + 1, right_index, center - length);
	}
}

/*
 * Initialise the internal state needed for sorting. This function is not needed for pure in-memory processing
 *
 */

void
sort_state_initialise(StatAggState *aggState, FunctionCallInfo fcinfo)
{
	Oid			collation;
	Oid			sortop,
				eqop,
				gtop;
	Type		t;
	MemoryContext oldctx;
	MemoryContext aggcontext;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "string_agg_transfn called in non-aggregate context");
	}

	oldctx = MemoryContextSwitchTo(aggcontext);
	get_sort_group_operators(aggState->valtype, true, true, true, &sortop, &eqop, &gtop, NULL);

	t = typeidType(aggState->valtype);
	collation = typeTypeCollation(t);
	ReleaseSysCache(t);
	aggState->sortstate = tuplesort_begin_datum(aggState->valtype, sortop, collation, SORTBY_NULLS_DEFAULT, work_mem, false);
	MemoryContextSwitchTo(oldctx);
}

/*
 * Initialise the state to be passed between the functions.
 *
 */


StatAggState *
state_initialise(FunctionCallInfo fcinfo)
{
	MemoryContext oldctx;
	MemoryContext aggcontext;
	int16		typlen;
	bool		typbyval;

	StatAggState *aggstate;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* cannot be called directly because of internal-type argument */
		elog(ERROR, "string_agg_transfn called in non-aggregate context");
	}

	oldctx = MemoryContextSwitchTo(aggcontext);

	aggstate = (StatAggState *) palloc(sizeof(StatAggState));
	aggstate->nelems = 0;
	aggstate->inmemory = true;

	aggstate->valtype = get_fn_expr_argtype(fcinfo->flinfo, 1);
	switch (aggstate->valtype)
	{
		case (INT8OID):
			break;
		case (INT4OID):
			break;
		case (INT2OID):
			break;
		case (FLOAT4OID):
			break;
		case (FLOAT8OID):
			break;
		default:
			aggstate->inmemory = false;
			break;
	}
	get_typlenbyval(aggstate->valtype, &typlen, &typbyval);
	aggstate->isTuple = !typbyval;
	aggstate->sortstate = NULL;
	aggstate->rawData = NULL;
	if (aggstate->inmemory)
	{
		aggstate->rawData = palloc(INMEMORYCAPACITY * sizeof(RawData));
	}

	MemoryContextSwitchTo(oldctx);

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
median_transfn(PG_FUNCTION_ARGS)
{
	int			iterator;

	StatAggState *aggstate;

	aggstate = PG_ARGISNULL(0) ? NULL : (StatAggState *) PG_GETARG_POINTER(0);

	if (!PG_ARGISNULL(1))
	{
		if (aggstate == NULL)
		{
			aggstate = state_initialise(fcinfo);
			sort_state_initialise(aggstate, fcinfo);
		}
		if (aggstate->inmemory)
		{
			if (aggstate->nelems == INMEMORYCAPACITY)
			{
				aggstate->inmemory = false;
				iterator = 0;
				for (iterator = 0; iterator < aggstate->nelems; iterator++)
				{
					tuplesort_putdatum(aggstate->sortstate, aggstate->rawData[iterator].datum, false);
				}
			}
			else
			{
				aggstate->rawData[aggstate->nelems].datum = PG_GETARG_DATUM(1);
				if (!aggstate->isTuple)
				{
					aggstate->rawData[aggstate->nelems].tuple = DatumGetPointer(aggstate->rawData[aggstate->nelems].datum);
				}
				else
				{
					aggstate->rawData[aggstate->nelems].tuple = NULL;
				}
			}
/*             tuplesort_putdatum(aggstate->sortstate, PG_GETARG_DATUM(1), false); */
			aggstate->nelems++;
		}
		else
		{
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
median_finalfn(PG_FUNCTION_ARGS)
{
	StatAggState *aggstate;

	Assert(AggCheckCallContext(fcinfo, NULL));

	aggstate = PG_ARGISNULL(0) ? NULL : (StatAggState *) PG_GETARG_POINTER(0);

	if (aggstate != NULL)
	{
		int			lidx;
		int			hidx;
		Datum		value;
		bool		isNull;
		int			i = 1;
		Datum		result = 0;

		hidx = aggstate->nelems / 2 + 1;
		lidx = (aggstate->nelems + 1) / 2;
		if (aggstate->inmemory)
		{
			result = quick_select(aggstate->rawData, 0, aggstate->nelems, lidx);
			if (aggstate->nelems > 1)
			{
				value = quick_select(aggstate->rawData, 0, aggstate->nelems, lidx + 1);
			}
		}
		else
		{
			tuplesort_performsort(aggstate->sortstate);
			while (tuplesort_getdatum(aggstate->sortstate,
									  true,
									  &value, &isNull, NULL))
			{
				if (i++ == lidx)
				{
					if (aggstate->valtype != CSTRINGOID && aggstate->valtype != VARCHAROID)
					{
						result = datumCopy(value, true, -1);
					}
					else
					{
						result = datumCopy(value, false, 0);
					}
					tuplesort_getdatum(aggstate->sortstate,
									   true,
									   &value, &isNull, NULL);
					break;
				}
			}
		}
		if (lidx != hidx)
		{
			switch (aggstate->valtype)
			{
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
		tuplesort_end(aggstate->sortstate);
		if (aggstate->rawData != NULL)
		{
			pfree(aggstate->rawData);
		}
		PG_RETURN_DATUM(result);
	}
	else
		PG_RETURN_NULL();
}
