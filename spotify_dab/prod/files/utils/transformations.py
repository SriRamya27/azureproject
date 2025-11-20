# deduplication
class reusable:
    def dropColumns(self, df, columns):
        #Without the *, PySpark would think you are passing one argument (a list), not multiple column names.
        # the * removes the list brackets [] and considers the column names - this is unpacking of list (to convert list to string). In order to convert string to list we use eval()
        df = df.drop(*columns) 
        return df