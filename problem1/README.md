Schema Decisions:
I used surrogate keys because they’re simple and consistent for joins. Natural keys could work, but surrogate IDs make things easier to maintain.

Constraints:
I added CHECK constraints like making sure passenger counts are non-negative, and UNIQUE constraints to prevent duplicate stop or trip names.

Complex Query:
The hardest one was the transfer-stop query because it needed a self-join to find stops shared by multiple lines.

Foreign Keys:
Foreign keys make sure data stays valid—for example, you can’t insert a stop event that refers to a line or trip that doesn’t exist.

When Relational:
SQL works great here because the data has clear relationships and structure, and we often need joins across multiple tables.