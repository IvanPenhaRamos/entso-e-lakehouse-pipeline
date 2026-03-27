def chop_date(execution_date: str)-> str:

	'''
	Parses a date string in YYYY-MM-DD format and returns year, month and day
	as strings without leading zeroes, matching Spark's partition naming convention.

	Args:
		execution_date (str): Date string in YYYY-MM-DD format.

	Returns:
		tuple: (year, month, day) as strings without leading zeroes.
	'''

	year = execution_date[:4]
	month = str(int(execution_date[5:7]))
	day = str(int(execution_date[8:10]))

	return (year, month, day)
