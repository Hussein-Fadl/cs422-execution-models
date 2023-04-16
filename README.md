# CS422 Project 1 - Relational Operators and Execution Models

This project covers five topics:
1. Semantics of Relational Operators,
2. Iterator Execution Model,
3. Execution using Late Materialization,
4. Simple Query Optimization Rules, and
5. Column-at-a-time Execution.

We briefly introduce the provided infrastructure, before presenting the project's tasks.

## Task 1: Implement a volcano-style tuple-at-a-time engine

We implement six basic operators (**Scan**,
**Select**, **Project**, **Join**,
**Aggregate** and **Sort**) in a volcano-style tuple-at-a-time operators by extending the provided
trait/interface `ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator`, where each operator processes a single tuple-at-a-time.
Each operator in a volcano-style engine requires the implementation of three methods:

* **open():** Initialize the operator's internal state.
* **next():** Process and return the next result tuple or NilTuple for `EOF`.
* **close():** Finalize the execution of the operator and clean up the allocated resources.

The **Scan** operator supports row-store storage layout (NSM).

The **Join** operator is implemented as **Hash Join (HJ)** using a hash map. Particularly, we load all the entries from the right branch (inner relation) and map them as a map entry, and then the join is performed based on the common keys between the right and the left. 

The **Filter** and **Project** operators are implemented that way:  One tuple is loaded at a time, pushed forward using Scan operator. A *predicate* is applied in case of Filter operator, and the *projection expression* is applied in case of a Project operator.

**Assumptions.** In the context of this project, we store all the data in-memory, thus we no longer require a buffer manager. In addition, we assume that the records will consist of objects (`Any` class in Scala).

## Task 2: Late Materialization

In this task, we implement late materialization.

Late materialization uses virtual IDs to reconstruct tuples on demand.
For example, suppose that our query plan evaluates the relational expression
σ<sub>A.x=5</sub>(A) ⨝<sub>A.y=B.y</sub> B. Without late materialization, the query engine
would execute the following steps:

* Scan columns A.x and A.y from table A
* Eliminate rows for which A.x!=5
* Join qualifying (A.x, A.y) tuples with table B on B.y

By contrast, with late materialization, the query engine
  would execute the following steps:

* Scan column A.x
* Eliminate rows for which A.x!=5
* For qualifying tuples, fetch A.y to reconstruct (A.x, A.y) tuples
* Join (A.x, A.y) tuples with table B on B.y

To implement late materialization, we enrich the query engine's tuples so that they
contain virtual IDs. We name an enriched tuple `LateTuple`. A `LateTuple` is
 structured as `LateTuple(vid : VID, tuple : Tuple)`


### Subtask 2.A: Implement Late Materialization primitive operators

In the first subtask, we implement the **Drop** and **Stitch** operators:

- Drop (`ch.epfl.dias.cs422.rel.early.volcano.late.Drop`) translates `LateTuple` to `Tuple` by dropping the virtual ID.
Drop allows interfacing late materialization-based execution with existing non-late materialized operators.
- Stitch (`ch.epfl.dias.cs422.rel.early.volcano.late.Stitch`) is a binary operator (has two input operators) and from the stream of `LateTuple` they provide, it synchronizes the two streams to produce a new stream of `LateTuple`.
  
  More specifically, the two inputs produce `LateTuple` for the same table but different groups of columns. These streams may or may not miss tuples, due to some pre-filtering. Stitch should find the virtual IDs that exist on both input streams and generate the output as `LateTuple` that includes the columns of both inputs (first the left input's  columns, then the right one's).

  __Example__: if the left input produces:

  ```LateTuple(2, Tuple("a")), LateTuple(8, Tuple("c"))```,

  and the right input produces:

  ```LateTuple(0, Tuple(3.0)), LateTuple(2, Tuple(6.0))```,

  Stitch should produce:

  ```LateTuple(2, Tuple("a", 6.0))```, since the only virtual IDs that both
  input streams share is 2.

### Subtask 2.B: Extend relational operators to support execution on LateTuple data

In the second subtask, we implement a

* *Filter* (`ch.epfl.dias.cs422.rel.early.volcano.late.LateFilter`)
* *Join* (`ch.epfl.dias.cs422.rel.early.volcano.late.LateJoin`)
* *Project* (`ch.epfl.dias.cs422.rel.early.volcano.late.LateProject`)

that directly operate on `LateTuple` data and, in the case of
`LateFilter` and `LateProject` preserve virtual IDs.

## Task 3: Query Optimization Rules

In this task, we implement new optimization rules to ''teach'' the query optimizer possible plan transformations.
Then we use these optimization rules to reduce data access in the query plans.

We use Apache Calcite as the query parser and optimizer. Apache Calcite is an open-source easy-to-extend Apache project,
used by many commercial and non-commercial systems. In this task, we implement a few new optimization rules that allow the parsed query to be transformed into a new query plan.

All optimization rules in Calcite inherit from RelOptRule and in `ch.epfl.dias.cs422.rel.early.volcano.late.qo`. Specifically, we implement `onMatchHelper`, which
computes a new sub-plan that replaces the pattern-matched sub-plan.

Note that these rules operate on top of Logical operators
and not the operators we implemented. The Logical operators are translated into our operators in a subsequent step of planning.

### Subtask 3.A: Implement the Fetch operator

Fetch (`ch.epfl.dias.cs422.rel.early.volcano.late.Fetch`) is a unary operator. It reads a stream of input `LateTuple`
and reconstructs missing columns by directly accessing the corresponding column's values. For example, assume that we are given
column A.y

```[ 5.0, 4.0, 8.0, 1.0 ]```

Also, assume a Fetch operator is used to reconstruct A.y for tuples

```LateTuple(1, Tuple("a")), LateTuple(2, Tuple("c"))```

Then, the result is

```LateTuple(1, Tuple("a", 4.0)), LateTuple(2, Tuple("c", 8.0))```

The advantage of fetch is that, unlike Stitch, it doesn't have to scan the full column A.y.

Also, Fetch optionally receives a list of expressions to compute over the reconstructed column. If no expressions are provided,
Fetch simply reconstructs the values of the column itself.

### Subtask 3.B: Implement the Optimization rules

In this subtask, we implement three rules:

- `ch.epfl.dias.cs422.rel.early.volcano.late.qo.LazyFetchRule` to replace a Stitch with a Fetch,
- `ch.epfl.dias.cs422.rel.early.volcano.late.qo.LazyFetchFilterRule` to replace a Stitch &rarr; Filter with a Fetch &rarr; Filter,
- `ch.epfl.dias.cs422.rel.early.volcano.late.qo.LazyFetchProjectRule` to replace a Stitch &rarr; Project with a Fetch.

Example: LazyFetchRule transforms the following subplan

Stitch

&rarr; Filter

&rarr; &rarr; LateColumnScan(A.x)

&rarr; LateColumnScan(A.y)

to

Fetch (A.y)

&rarr; Filter

&rarr; &rarr; LateColumnScan(A.x)

## Task 4: Execution Models

This tasks focuses on the column-at-a-time execution model, building gradually from an operator-at-a-time execution over columnar data.

### Subtask 4.A: Enable selection-vectors in operator-at-a-time execution

A fundamental block in implementing vector-at-a-time execution is selection-vectors. In this task, we implement the 

* **Filter** `ch.epfl.dias.cs422.rel.early.operatoratatime.Filter`
* **Project** `ch.epfl.dias.cs422.rel.early.operatoratatime.Project`
* **Join** `ch.epfl.dias.cs422.rel.early.operatoratatime.Join`
* **Scan** `ch.epfl.dias.cs422.rel.early.operatoratatime.Scan`
* **Sort** `ch.epfl.dias.cs422.rel.early.operatoratatime.Sort`
* **Aggregate** `ch.epfl.dias.cs422.rel.early.operatoratatime.Aggregate`

for operator-at-a-time execution over columnar
inputs. Our implementation is based on selection vectors and (`Tuple=>Tuple`) evaluators. That is, all operators receive one extra column of `Boolean`s (the last column) that signifies
which of the inputs tuples are active. The Filter, Scan, Project do not prune tuples, but only set the selection vector. 

### Subtask 4.B: Column-at-a-time with selection vectors and mapping functions

In this task, we implement:

* **Filter** (`ch.epfl.dias.cs422.rel.early.columnatatime.Filter`)
* **Project** (`ch.epfl.dias.cs422.rel.early.columnatatime.Project`)
* **Join** (`ch.epfl.dias.cs422.rel.early.columnatatime.Join`)
* **Scan** (`ch.epfl.dias.cs422.rel.early.columnatatime.Scan`)
* **Sort** (`ch.epfl.dias.cs422.rel.early.columnatatime.Sort`)
* **Aggregate** (`ch.epfl.dias.cs422.rel.early.columnatatime.Aggregate`)

for columnar-at-a-time execution over columnar inputs
with selection vectors, but this time instead of using the evaluators that work on tuples (`Tuple => Tuple`), we
use the `map`-based functions that evaluate one expression for the full
input (`Indexed[HomogeneousColumn] => HomogeneousColumn`).

__Note__: We convert a `Column` to `HomogeneousColumn` by using `toHomogeneousColumn()`.

## Project setup

### Setup your environment

The skeleton codebase is pre-configured for development in [IntelliJ (version 2020.3+)](https://www.jetbrains.com/idea/) and this is the only supported IDE. If you use
use any other IDE and/or IntelliJ version, it will be your responsibility to fix any configuration issues you encounter, including that through other IDEs may not display the provided documentation.
results.
