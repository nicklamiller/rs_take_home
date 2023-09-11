# Table of Contents
* [Problem Statement](#problem_statement)
* [Solution](#solution)

---

<a name="problem_statement"/>

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

Then all one needs to do is import `get_association_counts`:
    from rs_take_home.run import get_association_counts

    association_counts = get_association_counts()
    association_counts.show()

The function call above will run with defaults, which are the data files supplied in the `data` folder and the example queries given in the problem statement. This will return the example output also listed in the problem statement.
