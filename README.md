# CS422 Project 1 - Relational Operators and Execution Models

This project covers five topics:
a) the semantics of relational operators,
b) the iterator execution model,
c) execution using late materialization,
d) simple optimization rules, and
e) column-at-a-time execution.
We briefly introduce the provided infrastructure, before presenting the project's tasks.

## Infrastructure

<b> Register for access to the repository and grader. </b> <span style="color:red"> <b> ***DEADLINE 11/03/2022 10:00*** </b> </span>
For the projects we are going to be using https://gitlab.epfl.ch to distribute the skeleton code and accept the project submissions.
Thus we require you to register on https://gitlab.epfl.ch and submit your (sciper -> gitlab) username mapping in the following [link](https://moodle.epfl.ch/mod/questionnaire/view.php?id=1197404).

After the deadline we will provide you with a personal skeleton codebase https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2022/students/Project-1-username where <em>username</em> is your GitLab username.
In this repository you are going to be submitting your project and an automatic grader will be picking up your code to provide you feedback on your score.

We suggest you configure your ssh keys and register then in gitlab through your user profile settings,  SSH Keys (https://gitlab.epfl.ch/-/profile/keys). The same GitLab page provides further instruction on creating and adding keys.

## Task 1: Implement a volcano-style tuple-at-a-time engine (30%)

You will now implement six basic operators (**Scan**,
**Select**, **Project**, **Join**,
**Aggregate** and **Sort**) in a volcano-style tuple-at-a-time operators by extending the provided
trait/interface `ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator`, where each operator processes a single tuple-at-a-time.
As described in class, each operator in a volcano-style engine requires the implementation of three methods:

* **open():** Initialize the operator's internal state.
* **next():** Process and return the next result tuple or NilTuple for `EOF`.
* **close():** Finalize the execution of the operator and clean up the allocated resources.

You are free to implement the **Join** operator of your preference (we recommend
revising the slides from CS322).
The **Scan** operator need to support row-store storage layout (NSM).
Focus on correctness first, then try to optimize your implementation.

**Assumptions.** In the context of this project you will store all data in-memory, thus removing the requirement for a buffer manager. In addition, you can assume that the records will consist of objects (`Any` class in Scala).

**Important!!!** Implement the operators based on the prototypes given in the skeleton code.

__Hint__: we suggest that you start by looking the documentation and classes that appear in the boilerplate code e.g.
classes named Skeleton.*.
Rather than try to understand the whole codebase, focus on the task at hand.

## Task 2: Late Materialization (naive) (20%)

In this task, you will implement late materialization.

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

In the first subtask you should implement the Drop and Stitch operators:

- Drop ([ch.epfl.dias.cs422.rel.early.volcano.late.Drop]) translates `LateTuple` to `Tuple` by dropping the virtual ID.
Drop allows interfacing late materialization-based execution with existing non-late materialized operators.
- Stitch ([ch.epfl.dias.cs422.rel.early.volcano.late.Stitch]) is a binary operator (has two input operators)
  and from the stream of `LateTuple` they provide, it synchronizes the two streams to produce a new stream of `LateTuple`.
  More specifically, the two inputs produce `LateTuple` for the same table but different groups of columns. These streams
  may or may not miss tuples, due to some pre-filtering. Stitch should find the virtual IDs that exist on both
  input streams and generate the output as `LateTuple` that includes the columns of both inputs (first the left input's
  columns, then the right one's).

  __Example__: if the left input produces:

  ```LateTuple(2, Tuple("a")), LateTuple(8, Tuple("c"))```,

  and the right input produces:

  ```LateTuple(0, Tuple(3.0)), LateTuple(2, Tuple(6.0))```,

  Stitch should produce:

  ```LateTuple(2, Tuple("a", 6.0))```, since the only virtual IDs that both
  input streams share is 2.

### Subtask 2.B: Extend relational operators to support execution on LateTuple data

In the second subtask you should implement a

* *Filter* ([ch.epfl.dias.cs422.rel.early.volcano.late.LateFilter])
* *Join* ([ch.epfl.dias.cs422.rel.early.volcano.late.LateJoin])
* *Project* ([ch.epfl.dias.cs422.rel.early.volcano.late.LateProject])

that directly operate on `LateTuple` data and, in the case of
`LateFilter` and `LateProject` preserve virtual IDs.

## Task 3: Query Optimization Rules (20%)

In this task you will implement new optimization rules to ''teach'' the query optimizer possible plan transformations.
Then you are going to use these optimization rules to reduce data access in the query plans.

We use Apache Calcite as the query parser and optimizer. Apache Calcite is an open-source easy-to-extend Apache project,
used by many commercial and non-commercial systems. In this task, you are going to implement a few new optimization rules
that allow the parsed query to be transformed into a new query plan.

All optimization rules in Calcite inherit from RelOptRule and in [ch.epfl.dias.cs422.rel.early.volcano.late.qo] you can
find the boilerplate for the rules you are asked to implement. Specifically, you need to implement onMatchHelper, which
computes a new sub-plan that replaces the pattern-matched sub-plan.
Note that these rules operate on top of Logical operators
and not the operators you implemented. The Logical operators are translated into your operators in a subsequent step of planning.

### Subtask 3.A: Implement the Fetch operator

Fetch ([ch.epfl.dias.cs422.rel.early.volcano.late.Fetch]) is a unary operator. It reads a stream of input `LateTuple`
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

__Hint__: to test Fetch, you need to inject it to the plan (Subtask 3.B).

### Subtask 3.B: Implement the Optimization rules

You are called to implement three rules:

- [ch.epfl.dias.cs422.rel.early.volcano.late.qo.LazyFetchRule] to replace a Stitch with a Fetch,
- [ch.epfl.dias.cs422.rel.early.volcano.late.qo.LazyFetchFilterRule] to replace a Stitch &rarr; Filter with a Fetch &rarr; Filter,
- [ch.epfl.dias.cs422.rel.early.volcano.late.qo.LazyFetchProjectRule] to replace a Stitch &rarr; Project with a Fetch.

Example: LazyFetchRule transforms the following subplan

Stitch

&rarr; Filter

&rarr; &rarr; LateColumnScan(A.x)

&rarr; LateColumnScan(A.y)

to

Fetch (A.y)

&rarr; Filter

&rarr; &rarr; LateColumnScan(A.x)

## Task 4: Execution Models (30%)

This tasks focuses on the column-at-a-time execution model, building gradually from an operator-at-a-time execution over
columnar data.

### Subtask 4.A: Enable selection-vectors in operator-at-a-time execution

A fundamental block in implementing vector-at-a-time execution is selection-vectors. In this task you should implement
the 

* **Filter** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Filter])
* **Project** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Project])
* **Join** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Join])
* **Scan** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Scan])
* **Sort** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Sort])
* **Aggregate** ([ch.epfl.dias.cs422.rel.early.operatoratatime.Aggregate]) 

for operator-at-a-time execution over columnar
inputs. Your implementation should be based on selection vectors and (`Tuple=>Tuple`) evaluators. That is, all operators receive one extra column of `Boolean`s (the last column) that signifies
which of the inputs tuples are active. The Filter, Scan, Project should not prune tuples, but only set the selection
vector. For the Join and Aggregate you are free to select whether they only generate active tuples or they also produce
inactive tuples, as long as you conform with the operator interface (extra Boolean column).

### Subtask 4.B: Column-at-a-time with selection vectors and mapping functions

In this task you should implement 

* **Filter** ([ch.epfl.dias.cs422.rel.early.columnatatime.Filter])
* **Project** ([ch.epfl.dias.cs422.rel.early.columnatatime.Project])
* **Join** ([ch.epfl.dias.cs422.rel.early.columnatatime.Join])
* **Scan** ([ch.epfl.dias.cs422.rel.early.columnatatime.Scan])
* **Sort** ([ch.epfl.dias.cs422.rel.early.columnatatime.Sort])
* **Aggregate** ([ch.epfl.dias.cs422.rel.early.columnatatime.Aggregate])

for columnar-at-a-time execution over columnar inputs
with selection vectors, but this time instead of using the evaluators that work on tuples (`Tuple => Tuple`), you should
use the `map`-based provided functions that evaluate one expression for the full
input (`Indexed[HomogeneousColumn] => HomogeneousColumn`).

__Hint__: You can convert a `Column` to `HomogeneousColumn` by using `toHomogeneousColumn()`.

## Project setup & grading

### Setup your environment

The skeleton codebase is pre-configured for development in [IntelliJ (version 2020.3+)](https://www.jetbrains.com/idea/) and this is the only supported IDE. You are free to
use any other IDE and/or IntelliJ version, but it will be your sole responsibility to fix any configuration issues you
encounter, including that through other IDEs may not display the provided documentation.

After you install IntelliJ in your machine, from the File menu select
`New->Project from Version Control`. Then on the left-hand side panel pick `Repository URL`. On the right-hand side
pick:

* Version control: Git
* URL: [https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2022/students/Project-1-username](https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2022/students/)
or [git@gitlab.epfl.ch:DIAS/COURSES/CS-422/2022/students/Project-1-username](git@gitlab.epfl.ch:DIAS/COURSES/CS-422/2022/students/)
, depending on whether you set up SSH keys (where <username> is your GitLab username).
* Directory: anything you prefer, but in the past we have seen issues with non-ascii code paths (such as french
  punctuations), spaces and symlinks

IntelliJ will clone your repository and setup the project environment. If you are prompt to import or auto-import the
project, please accept. If the JDK is not found, please use IntelliJ's option to `Download JDK`, so that IntelliJ
install the JDK in a location that will not change your system settings and the IDE will automatically configure the
project paths to point to this JDK.

### Personal repository

The provided
repository ([https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2022/students/Project-1-username](https://gitlab.epfl.ch/DIAS/COURSES/CS-422/2022/students/))
is personal and you are free to push code/branches as you wish. The grader will run on all the branches, but for the
final submission only the master branch will be taken into consideration.

### Additional information and documentation

The skeleton code depends on a library we provide to integrate the project with a state-of-the-art query optimizer,
Apache Calcite. Additional information for Calcite can be found in it's official
site [https://calcite.apache.org](https://calcite.apache.org)
and it's documentation site [https://calcite.apache.org/javadocAggregate/](https://calcite.apache.org/javadocAggregate/).

Documentation for the integration functions and helpers we provide as part of the project-to-Calcite integration code
can be found either be browsing the javadoc of the dependency jar (External Libraries/ch.epfl.dias.cs422:base), or by
browsing to
[http://diascld24.iccluster.epfl.ch:8080/ch/epfl/dias/cs422/helpers/index.html](http://diascld24.iccluster.epfl.ch:8080/ch/epfl/dias/cs422/helpers/index.html)
WHILE ON VPN.

*If while browsing the code IntelliJ shows a block:*

```scala
/**
 * @inheritdoc
 */

```

Next to it, near the column with the file numbers, the latest versions of IntelliJ have a paragraph symbol
to `Toggle Render View` (to Reader Mode) and get IntelliJ to display the properly rendered inherited prettified
documentation.
*In addition to the documentation in inheritdoc, you may want to browse the documentation of parent classes (
including the skeleton operators and the parent Operator and [ch.epfl.dias.cs422.helpers.rel.RelOperator] classes)*

***Documentation of constructor's input arguments and examples are not copied by the IntelliJ's inheritdoc command, so
please visit the parent classes for such details***

### Submissions & deliverables

Submit your code and short report, by pushing it to your personal gitlab project before the deadline. The repositories
will be frozen after the deadline and we are going to run the automated grader on the final tests.

We will grade the last commit on the `master` branch of your GitLab repository. In the context of this project you only
need to modify the ``ch.epfl.dias.cs422.rel'' package. Except from the credential required to get access to the
repository, there is nothing else to submit on moodle for this project. Your repository must contain a `Report.pdf` or
`report.md` which is a short report that gives a small overview of the peculiarities of your implementation and any
additional details/notes you want to submit. If you submit the report in markdown format, you are responsible for making
sure it renders properly on gitlab.epfl.ch's preview.

To evaluate your solution, run your code with the provided tests ([ch.epfl.dias.cs422.QueryTest] class).

#### Grading

Keep in mind that we will test your code automatically.
Any project that fails to conform to the original skeleton code
and interfaces will fail in the auto grader, and hence, will be graded as a zero.
More specifically, you should not change the function and constructor signatures provided in the skeleton code, or make any other change that will break interoperability with the base library.

You are allowed to add new classes, files and packages, but only under the current package. Any code outside the current
package will be ignored and not graded. You are free to edit the `Main.scala` file and/or create new `tests`, but we are
going to ignore such changes during grading.

Tests that timeout will lose all the points for the timed-out test cases, as if they returned wrong results.
