#branch tw1555
Contains code for cleaning columns with type string.
All columns have different constraints imposed on them, such as a regex string, a dictionary, etc.
If a column which expects a string value actually contains an int, datetime, or float value, the type detected is 
displayed along with an 'invalid' indicator along with it.
Columns which we think should not have null values, like an address, a complaint type, or an agency name, 
do not allow null/blanks. The blanks are declared invalid.
Columns which could accomodate some empty entries, like "Ferry Direction" or "School Name" assign "N/A" to blanks
instead of declaring them invalid. So, instead of "invalid", they are given "N/A".
