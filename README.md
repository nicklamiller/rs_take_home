## Table of Contents
* [Problem](#problem)
* [Solution](#solution)
* [Further Discussion](#further_discussion)

---

<a name="problem"/>

## Problem
### Introduction
In the field of genomics, understanding the relationships between genes and diseases is crucial. This task revolves
around assessing these relationships, especially when considering hierarchical disease ontology.

### Problem Statement

Write a Python application that takes as input:
  1. list of gene-disease associations,
  2. disease hierarchy,
  3. single gene-disease association pair as a query

And should output the number of unique genes associated with the query disease or any of its direct parent/child
diseases, based on the input data.

Input gene-disease associations and disease hierarchy should:
 * adhere to the format in the data directory
 * utilize Ensembl IDs (e.g. `ENSG00000101342`) for genes and EFO IDs (e.g. `MONDO:0019557`) for diseases
 * define the valid gene and disease space

Explicitly state any assumptions in your code comments or documentation. Enhance or modify the given template as needed
to ensure clarity and maintainability. You're encouraged to use standard third-party Python libraries or frameworks that
are widely recognized. Feel free to include anything that you think clarifies or improves your solution. While we have
included example disease and gene labels in the `data` directory, using them is optional.

### Example

Given the example data in the `data` directory, here's some examples of queries and results:

| Query | Result |
| --- | --- |
| (ENSG00000101342, MONDO:0019557) | 4 |
| (ENSG00000101347, MONDO:0015574) | 3 |
| (ENSG00000213689, MONDO:0019557) | 4 |
| (ENSG00000213689, MONDO:0018827) | 3 |

### Considerations

The solution will be graded holistically, based on correctness, efficiency, readability and software practices.

Some things worth considering (though not necessarily crucial):
 * What should be done about inconsistent or corrupted data/queries?
 * How would you optimize the solution for a large number of gene-disease queries?
 * Parallelization? Caching?


### Solution Submission

Please send back your solution as a zip file containing the code and any other files you think are relevant. If you
prefer to send a link to a GitHub repository with your solution instead of the zip file, that's also acceptable.
Note that the solution should be self-contained and include any necessary instructions to use it.

---

<a name="solution"/>

## Solution
To run the solution to this test, one can install this repo as a package:

    pip install git+https://github.com/nicklamiller/rs_take_home.git

Then all one needs to do is import `get_association_counts` and supply filepaths to data. If you don't have the data readily available feel free to run these commands to get the data locally:

    mkdir data
    curl -o "data/example_associations.csv" https://raw.githubusercontent.com/nicklamiller/rs_take_home/add-explanation-to-readme/data/example_associations.csv
    curl -o "data/example_disease_hierarchy.csv" https://raw.githubusercontent.com/nicklamiller/rs_take_home/add-explanation-to-readme/data/example_disease_hierarchy.csv


And then these commands in an interactive python session to use `get_association_counts`:

    from rs_take_home.run import get_association_counts


    gene_disease_associations_path = 'data/example_associations.csv'
    disease_hierarchy_path = 'data/example_disease_hierarchy.csv'
    queries = [('ENSG00000101342', 'MONDO:0019557'), ('ENSG00000101347', 'MONDO:0015574')]

    association_counts = get_association_counts(
        gene_disease_associations_path=gene_disease_associations_path,
        disease_hierarchy_path=disease_hierarchy_path,
    )
    association_counts.show()

If can also supply queries as an argument with a list of tuples that correspond to custom queries. The default queries used here are those specified in the problem statement and will return the example output also listed in the problem statement.

---

<a name="further_discussion"/>

## Further Discussion

### Considerations:

* Corruped data/queries:

There is built in validation when one supplies their own data/filepaths to data, so these dataframes will have to contain the correct columns and datatypes as specified in `rs_take_home.schemas` (they can include additional columns as well). Given more time I would like to add validation for the Ensembl and EFO ID's for the queries using regex patterns.

* Optimize solution:

This code is written using Pyspark. Because it was ran on my laptop, it is ran in SingleNode cluster mode, but one could supply a SparkSession with custom configurations that include multiple workers and/or a higher spec driver/workers. This spark session can then be passed as an argument to the `get_association_counts` function to help scale this solution.

* Parallelization:

As mentioned above, one could choose a different spark session configuration that involves multiple nodes/workers and pass this spark session to the `get_association_counts` function to better parallelize the code.


### Tools used in this repo:

This repo is made from a [CI/CD template](https://github.com/nicklamiller/CICD_template) I've created that includes several developer tools I've found useful. Among these include:

* Automated tests:
  * Linting - flake8 and wemake-python-styleguide are used
  * Unit tests - pytest frame work for testing public methods
* Virtual environment management/ package building - poetry is used to manage dependencies and to build this repo as a package using masonry
* Precommit hooks - file and link checks as specified in `.pre-commit-config.yaml`, as well as calling the automated linter
* Input/data validation - pydantic is used to both validate attributes of classes upon instantiation, and to validate the contents of those attributes. In this case I validate the schemas of the input dataframes.
* Incremental PR's - code is developed incrementally and a PR is opened for code review (of course for this I was the only reviewer 😅 but for non-take-home-test projects I would gladly tag peers and get their feedback).
  * I would also normally create tickets on Jira, use these tickets to prefix branch names, and use Atlassian's [github-for-jira](https://github.com/atlassian/github-for-jira) integration so that branches/PR's are tracked on Jira (naturally other project management tools like Azure Boards offer github integration and could be used similarly).
