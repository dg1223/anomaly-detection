from pyspark.ml import UnaryTransformer
from pyspark.sql.types import StringType


class CnExtractor(UnaryTransformer):
    def __init__(self, setInputCol, setOutputCol):
        super(CnExtractor, self).__init__()
        self.setOutputCol(setOutputCol)
        self.setInputCol(setInputCol)

    def outputDataType(self):
        return StringType()

    def validateInputType(self, inputType) -> None:
        if inputType != StringType():
            raise Exception("Invalid inputType")

    def cleanUsername(self, row: str) -> str:
        row = row.split(",", 2)[0]
        if "cn=" in row:
            return row.split("=")[1]
        else:
            return row

    def createTransformFunc(self):
        return self.cleanUsername
