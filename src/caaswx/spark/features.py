from src.caaswx.spark.utils import (
    HasTypedInputCol,
    HasTypedInputCols,
    HasTypedOutputCol,
)

from src.caaswx.spark.base import (
    GroupbyFeature,
    CounterFeature,
    DistinctCounterFeature,
    ArrayDistinctFeature,
    ArrayRemoveFeature,
    SizeArrayRemoveFeature,
)

from pyspark.sql.functions import (
    col,
    when,
    lag,
    isnull,
    regexp_extract,
    countDistinct,
    array_distinct,
    sort_array,
    collect_set,
    collect_list,
    mean as sparkmean,
    stddev as sparkstddev,
    min as sparkmin,
    max as sparkmax,
    round as sparkround,
    sum as sparksum,
    slice as sparkslice,
)
from pyspark.ml.param.shared import TypeConverters, Param, Params
from pyspark.sql.types import (
    IntegerType,
    LongType,
    ArrayType,
    TimestampType,
    StringType,
    StructType,
    StructField,
)
from pyspark.sql.window import Window


class CountAuthAccept(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Authentication Accept events(sm_eventid == 1).
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AUTH_ACCEPT"):
        super(CountAuthAccept, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_AUTH_ACCEPT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 1.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 1, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAuthReject(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Authentication Reject events(sm_eventid == 2).
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AUTH_REJECT"):
        super(CountAuthReject, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_AUTH_REJECT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 2.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 2, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAdminAttempt(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Admin Attempt events(sm_eventid == 3).
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_ATTEMPT"):
        super(CountAdminAttempt, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_ATTEMPT"
        )
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 3.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 3, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAuthChallenge(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Authentication Challenge events
    (sm_eventid == 4).
    """

    def __init__(
        self, inputCol="SM_EVENTID", outputCol="COUNT_AUTH_CHALLENGE"
    ):
        super(CountAuthChallenge, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_EVENTID", outputCol="COUNT_AUTH_CHALLENGE"
        )
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 4.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 4, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAZAccept(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of AZ Accept events(sm_eventid == 5).
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AZ_ACCEPT"):
        super(CountAZAccept, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_AZ_ACCEPT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 5.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 5, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAZReject(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Authentication Reject events(sm_eventid == 6).
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AZ_REJECT"):
        super(CountAZReject, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_AZ_REJECT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 6.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 6, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAdminLogin(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Admin Login events(sm_eventid == 7).
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_LOGIN"):
        super(CountAdminLogin, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_LOGIN")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 7.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 7, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAdminLogout(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Admin Logout events(sm_eventid == 8).
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_LOGOUT"):
        super(CountAdminLogout, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_LOGOUT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 8.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 8, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAdminReject(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Admin Reject events(sm_eventid == 9).
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_REJECT"):
        super(CountAdminReject, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_REJECT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 9.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 9, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAuthLogout(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Authentication Logout events(sm_eventid == 10).
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AUTH_LOGOUT"):
        super(CountAuthLogout, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_AUTH_LOGOUT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 10.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 10, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountValidateAccept(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Validate Accept events(sm_eventid == 11).
    """

    def __init__(
        self, inputCol="SM_EVENTID", outputCol="COUNT_VALIDATE_ACCEPT"
    ):
        super(CountValidateAccept, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_EVENTID", outputCol="COUNT_VALIDATE_ACCEPT"
        )
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 11.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 11, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountValidateReject(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Validate Reject events(sm_eventid == 12).
    """

    def __init__(
        self, inputCol="SM_EVENTID", outputCol="COUNT_VALIDATE_REJECT"
    ):
        super(CountValidateReject, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_EVENTID", outputCol="COUNT_VALIDATE_REJECT"
        )
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 12.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 12, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountVisit(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Visit events(sm_eventid == 13).
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_VISIT"):
        super(CountVisit, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_VISIT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if EVENTID == 13.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 13, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountFailed(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of Failed events(sm_eventid == 2 or 6 or 9).
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_FAILED"):
        super(CountFailed, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_FAILED")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 2 or 6 or 9.
        :rtype: BooleanType
        """
        return when(
            (
                (col(self.getOrDefault("inputCol")) == 2)
                | (col(self.getOrDefault("inputCol")) == 6)
                | (col(self.getOrDefault("inputCol")) == 9)
            ),
            True,
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountOUAms(CounterFeature, HasTypedInputCols):
    """
    Feature calculates count of "ams", "AMS" occurrences in inputCols
    (default=(SM_USERNAME, SM_RESOURCE)).
    """

    def __init__(
        self,
        inputCols=["SM_USERNAME", "SM_RESOURCE"],
        outputCol="COUNT_OU_AMS",
    ):
        super(CountOUAms, self).__init__(outputCol)
        self._setDefault(
            inputCols=["SM_USERNAME", "SM_RESOURCE"], outputCol="COUNT_OU_AMS"
        )
        self._set(
            inputCols=["SM_USERNAME", "SM_RESOURCE"],
            inputColsType=[StringType(), StringType()],
        )
        sch_list = []
        for x, y in zip(
            self.getOrDefault("inputCols"), self.getOrDefault("inputColsType")
        ):
            sch_list.append(StructField(x, y))
        schema = StructType(sch_list)
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True when ams or AMS is found in inputCols
        :rtype: BooleanType
        """
        return when(
            (
                (col(self.getOrDefault("inputCols")[0]).contains("ams"))
                | (col(self.getOrDefault("inputCols")[1]).contains("AMS"))
            ),
            True,
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountOUCms(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of occurrences of "cra-cp" in inputCol
    (default=SM_USERNAME).
    """

    def __init__(self, inputCol="SM_USERNAME", outputCol="COUNT_OU_CMS"):
        super(CountOUCms, self).__init__(outputCol)
        self._setDefault(inputCol="SM_USERNAME", outputCol="COUNT_OU_AMS")
        self._set(inputCol="SM_USERNAME", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True when cra-cp is found in inputCols
        :rtype: BooleanType
        """
        return when(
            ((col(self.getOrDefault("inputCol")).contains("cra-cp"))), True
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountGet(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of occurrences of "GET" in inputCol
    (default=SM_ACTION).
    """

    def __init__(self, inputCol="SM_ACTION", outputCol="COUNT_GET"):
        super(CountGet, self).__init__(outputCol)
        self._setDefault(inputCol="SM_ACTION", outputCol="COUNT_GET")
        self._set(inputCol="SM_ACTION", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True when "GET" in SM_ACTION
        :rtype: BooleanType
        """
        return when((col(self.getOrDefault("inputCol")).contains("GET")), True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountPost(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of occurrences of "POST" in inputCol
    (default=SM_ACTION).
    """

    def __init__(self, inputCol="SM_ACTION", outputCol="COUNT_POST"):
        super(CountPost, self).__init__(outputCol)
        self._setDefault(inputCol="SM_ACTION", outputCol="COUNT_POST")
        self._set(inputCol="SM_ACTION", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True when "POST" in SM_ACTION
        :rtype: BooleanType
        """
        return when(
            (col(self.getOrDefault("inputCol")).contains("POST")), True
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountHTTPMethod(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of occurrences of "GET" or "POST" in inputCol
    (default=SM_ACTION).
    """

    def __init__(self, inputCol="SM_ACTION", outputCol="COUNT_HTTP_METHOD"):
        super(CountHTTPMethod, self).__init__(outputCol)
        self._setDefault(inputCol="SM_ACTION", outputCol="COUNT_HTTP_METHOD")
        self._set(inputCol="SM_ACTION", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True when "POST" or "GET" in SM_ACTION
        :rtype: BooleanType
        """
        return when(
            (col(self.getOrDefault("inputCol")).contains("GET"))
            | (col(self.getOrDefault("inputCol")).contains("POST")),
            True,
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountOUIdentity(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of occurrences of "ou=Identity" in inputCol
    (default=SM_USERNAME).
    """

    def __init__(self, inputCol="SM_USERNAME", outputCol="COUNT_OU_IDENTITY"):
        super(CountOUIdentity, self).__init__(outputCol)
        self._setDefault(inputCol="SM_USERNAME", outputCol="COUNT_OU_IDENTITY")
        self._set(inputCol="SM_USERNAME", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True when "ou=Identity" in SM_USERNAME
        :rtype: BooleanType
        """
        return when(
            col(self.getOrDefault("inputCol")).contains("ou=Identity"),
            True,
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountOUCred(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of occurrences of "ou=Credential" in inputCol
    (default=SM_USERNAME).
    """

    def __init__(self, inputCol="SM_USERNAME", outputCol="COUNT_OU_CRED"):
        super(CountOUCred, self).__init__(outputCol)
        self._setDefault(inputCol="SM_USERNAME", outputCol="COUNT_OU_CRED")
        self._set(inputCol="SM_USERNAME", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True when "ou=Credential" in SM_USERNAME
        :rtype: BooleanType
        """
        return when(
            col(self.getOrDefault("inputCol")).contains("ou=Credential"),
            True,
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountOUSecurekey(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of occurrences of "ou=SecureKey" in inputCol
    (default=SM_USERNAME).
    """

    def __init__(self, inputCol="SM_USERNAME", outputCol="COUNT_OU_SECUREKEY"):
        super(CountOUSecurekey, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_USERNAME", outputCol="COUNT_OU_SECUREKEY"
        )
        self._set(inputCol="SM_USERNAME", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True when "ou=SecureKey" in SM_USERNAME
        :rtype: BooleanType
        """
        return when(
            col(self.getOrDefault("inputCol")).contains("ou=SecureKey"),
            True,
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountPortalMya(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of occurrences of "mima" in inputCol
    (default=SM_RESOURCE).
    """

    def __init__(self, inputCol="SM_RESOURCE", outputCol="COUNT_PORTAL_MYA"):
        super(CountPortalMya, self).__init__(outputCol)
        self._setDefault(inputCol="SM_RESOURCE", outputCol="COUNT_PORTAL_MYA")
        self._set(inputCol="SM_RESOURCE", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True when "mima" in SM_RESOURCE
        :rtype: BooleanType
        """
        return when(
            col(self.getOrDefault("inputCol")).contains("mima"),
            True,
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountPortalMyba(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of occurrences of "myba" in inputCol
    (default=SM_RESOURCE).
    """

    def __init__(self, inputCol="SM_RESOURCE", outputCol="COUNT_PORTAL_MYBA"):
        super(CountPortalMyba, self).__init__(outputCol)
        self._setDefault(inputCol="SM_RESOURCE", outputCol="COUNT_PORTAL_MYBA")
        self._set(inputCol="SM_RESOURCE", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True when "myba" in SM_RESOURCE
        :rtype: BooleanType
        """
        return when(
            col(self.getOrDefault("inputCol")).contains("myba"),
            True,
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountRecords(CounterFeature, HasTypedInputCol):
    """
    Feature passes inputCol(default=CRA_SEQ) column to base feature to count
    number of elements.
    """

    def __init__(self, inputCol="CRA_SEQ", outputCol="COUNT_RECORDS"):
        super(CountRecords, self).__init__(outputCol)
        self._setDefault(inputCol="CRA_SEQ", outputCol="COUNT_RECORDS")
        self._set(inputCol="CRA_SEQ", inputColType=LongType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: CRA_SEQ column
        :rtype: :class:`pyspark.sql.Column'
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UserLoginAttempts(CounterFeature, HasTypedInputCol):
    """
    Feature passes True when a User Login Attempt event occurs defined by
    1 >= sm_eventid <= 6, otherwise returns None.
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="UserLoginAttempts"):
        super(UserLoginAttempts, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="UserLoginAttempts")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True if 1 >= EVENTID <= 6.
        :rtype: BooleanType
        """
        return when(
            (
                (col(self.getOrDefault("inputCol")) >= 1)
                | (col(self.getOrDefault("inputCol")) <= 6)
            ),
            True,
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UserNumOfPasswordChange(CounterFeature, HasTypedInputCol):
    """
    Feature calculates count of occurrences of "changePassword" in inputCol
    (default=SM_RESOURCE).
    """

    def __init__(
        self, inputCol="SM_RESOURCE", outputCol="UserNumOfPasswordChange"
    ):
        super(UserNumOfPasswordChange, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_RESOURCE", outputCol="UserNumOfPasswordChange"
        )
        self._set(inputCol="SM_RESOURCE", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns True when "changePassword" in SM_RESOURCE
        :rtype: BooleanType
        """
        return when(
            ((col(self.getOrDefault("inputCol")).contains("changePassword"))),
            True,
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountUniqueActions(DistinctCounterFeature, HasTypedInputCol):
    """
    Feature calculates count of distinct Actions in
    inputCol (default=SM_ACTION).
    """

    def __init__(self, inputCol="SM_ACTION", outputCol="COUNT_UNIQUE_ACTIONS"):
        super(CountUniqueActions, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_ACTION", outputCol="COUNT_UNIQUE_ACTIONS"
        )
        self._set(inputCol="SM_ACTION", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns the column SM_ACTION
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountUniqueEvents(DistinctCounterFeature, HasTypedInputCol):
    """
    Feature calculates count of distinct Events in inputCol
    (default=SM_EVENTID).
    """

    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_UNIQUE_EVENTS"):
        super(CountUniqueEvents, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_EVENTID", outputCol="COUNT_UNIQUE_EVENTS"
        )
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns the column SM_EVENTID
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountUniqueSessions(DistinctCounterFeature, HasTypedInputCol):
    """
    Feature calculates count of distinct Sessions in inputCol
    (default=SM_SESSIONID).
    """

    def __init__(
        self, inputCol="SM_SESSIONID", outputCol="COUNT_UNIQUE_SESSIONS"
    ):
        super(CountUniqueSessions, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_SESSIONID", outputCol="COUNT_UNIQUE_SESSIONS"
        )
        self._set(inputCol="SM_SESSIONID", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns the column SM_SESSIONID
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountUniqueUsername(DistinctCounterFeature, HasTypedInputCol):
    """
    Feature calculates count of distinct Usernames in inputCol(default=CN).
    """

    def __init__(self, inputCol="CN", outputCol="COUNT_UNIQUE_USERNAME"):
        super(CountUniqueUsername, self).__init__(outputCol)
        self._setDefault(inputCol="CN", outputCol="COUNT_UNIQUE_USERNAME")
        self._set(inputCol="CN", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns the column CN
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountUniqueResources(DistinctCounterFeature, HasTypedInputCol):
    """
    Feature calculates count of distinct Resources in inputCol
    (default=SM_RESOURCE).
    """

    def __init__(
        self, inputCol="SM_RESOURCE", outputCol="COUNT_UNIQUE_RESOURCES"
    ):
        super(CountUniqueResources, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_RESOURCE", outputCol="COUNT_UNIQUE_RESOURCES"
        )
        self._set(inputCol="SM_RESOURCE", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns column SM_RESOURCE
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountUniqueIps(DistinctCounterFeature, HasTypedInputCol):
    """
    Feature calculates count of distinct IPs in inputCol(default=SM_CLIENTIP).
    """

    def __init__(self, inputCol="SM_CLIENTIP", outputCol="COUNT_UNIQUE_IPS"):
        super(CountUniqueIps, self).__init__(outputCol)
        self._setDefault(inputCol="SM_CLIENTIP", outputCol="COUNT_UNIQUE_IPS")
        self._set(inputCol="SM_CLIENTIP", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def count_clause(self):
        """
        :return: Returns column SM_RESOURCE
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class MinUserTimestamp(GroupbyFeature, HasTypedInputCol, HasTypedOutputCol):
    """
    Feature calculates the first timestamp in the given group.
    """

    def __init__(
        self, inputCol="SM_TIMESTAMP", outputCol="MIN_USER_TIMESTAMP"
    ):
        super(MinUserTimestamp, self).__init__()
        self._setDefault(
            inputCol="SM_TIMESTAMP", outputCol="MIN_USER_TIMESTAMP"
        )
        self._set(
            inputCol="SM_TIMESTAMP",
            inputColType=TimestampType(),
            outputCol=outputCol,
            outputColType=IntegerType(),
        )
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def agg_op(self):
        return sparkmin(col(self.getOrDefault("inputCol"))).alias(
            self.getOutputCol()
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class MinTimeBtRecords(GroupbyFeature, HasTypedInputCols, HasTypedOutputCol):
    """
    Feature calculates the smallest time gap between two consecutive records
    in the group.

    Example:
    +--------+------------------------------+
    | User   | Timestamp                    |
    +========+==============================+
    | User A | 2018-01-01T00:00:04.100+0000 |
    |        | 2018-01-01T00:00:04.200+0000 |
    |        | 2018-01-01T00:00:04.250+0000 |
    |        | 2018-01-01T00:00:04.600+0000 |
    +--------+------------------------------+

    2018-01-01T00:00:04.200+0000 -> 2018-01-01T00:00:04.250+0000 is the
    smallest gap.

    MinTimeBtRecords = 2018-01-01T00:00:04.200+0000
                        - 2018-01-01T00:00:04.250+0000
                     = 0.05

    """

    def __init__(
        self,
        inputCols=["SM_TIMESTAMP", "CN"],
        outputCol="MIN_TIME_BT_RECORDS",
    ):
        super(MinTimeBtRecords, self).__init__()
        self._setDefault(
            inputCols=["SM_TIMESTAMP", "CN"],
            outputCol="MIN_TIME_BT_RECORDS",
        )
        self._set(
            inputCols=["SM_TIMESTAMP", "CN"],
            inputColsType=[TimestampType(), StringType()],
            outputCol=outputCol,
            outputColType=IntegerType(),
        )
        sch_list = []
        for x, y in zip(
            self.getOrDefault("inputCols"), self.getOrDefault("inputColsType")
        ):
            sch_list.append(StructField(x, y))
        schema = StructType(sch_list)
        self.set_input_schema(schema)

    def agg_op(self):
        return sparkmin(col(self.getOrDefault("inputCols")[0])).alias(
            self.getOutputCol()
        )

    def pre_op(self, dataset):
        if "SM_CONSECUTIVE_TIME_DIFFERENCE" not in dataset.columns:
            ts_window = Window.partitionBy(
                self.getOrDefault("inputCols")[1]
            ).orderBy("SM_TIMESTAMP")
            dataset = dataset.withColumn(
                "SM_PREV_TIMESTAMP",
                lag(dataset["SM_TIMESTAMP"]).over(ts_window),
            )

            dataset = dataset.withColumn(
                "SM_CONSECUTIVE_TIME_DIFFERENCE",
                when(
                    isnull(
                        dataset["SM_TIMESTAMP"].cast("long")
                        - dataset["SM_PREV_TIMESTAMP"].cast("long")
                    ),
                    0,
                ).otherwise(
                    dataset["SM_TIMESTAMP"].cast("long")
                    - dataset["SM_PREV_TIMESTAMP"].cast("long")
                ),
            )

            dataset = dataset.drop("SM_PREV_TIMESTAMP")
        return dataset

    def post_op(self, dataset):
        return dataset


class MaxUserTimestamp(GroupbyFeature, HasTypedInputCol, HasTypedOutputCol):
    """
    Feature calculates the last timestamp in the given group.
    """

    def __init__(
        self, inputCol="SM_TIMESTAMP", outputCol="MAX_USER_TIMESTAMP"
    ):
        super(MaxUserTimestamp, self).__init__()
        self._setDefault(
            inputCol="SM_TIMESTAMP", outputCol="MAX_USER_TIMESTAMP"
        )
        self._set(
            inputCol="SM_TIMESTAMP",
            inputColType=TimestampType(),
            outputCol=outputCol,
            outputColType=IntegerType(),
        )
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def agg_op(self):
        return sparkmax(col(self.getOrDefault("inputCol"))).alias(
            self.getOutputCol()
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class MaxTimeBtRecords(GroupbyFeature, HasTypedInputCols, HasTypedOutputCol):
    """
    Feature calculates the largest time gap between consecutive time entries
    in the group.

    Example:
    +--------+------------------------------+
    | User   | Timestamp                    |
    +========+==============================+
    | User A | 2018-01-01T00:00:04.100+0000 |
    |        | 2018-01-01T00:00:04.200+0000 |
    |        | 2018-01-01T00:00:04.250+0000 |
    |        | 2018-01-01T00:00:04.600+0000 |
    +--------+------------------------------+

    2018-01-01T00:00:04.250+0000 -> 2018-01-01T00:00:04.600+0000 is the
    largest time gap.

    MaxTimeBtRecords = 2018-01-01T00:00:04.250+0000
                        - 2018-01-01T00:00:04.600+0000
                     = 0.35
    """

    def __init__(
        self,
        inputCols=["SM_TIMESTAMP", "CN"],
        outputCol="MAX_TIME_BT_RECORDS",
    ):
        super(MaxTimeBtRecords, self).__init__()
        self._setDefault(
            inputCols=["SM_TIMESTAMP", "CN"],
            outputCol="MAX_TIME_BT_RECORDS",
        )
        self._set(
            inputCols=["SM_TIMESTAMP", "CN"],
            inputColsType=[TimestampType(), StringType()],
            outputCol=outputCol,
            outputColType=IntegerType(),
        )
        sch_list = []
        for x, y in zip(
            self.getOrDefault("inputCols"), self.getOrDefault("inputColsType")
        ):
            sch_list.append(StructField(x, y))
        schema = StructType(sch_list)
        self.set_input_schema(schema)

    def agg_op(self):
        return sparkmax(col(self.getOrDefault("inputCols")[0])).alias(
            self.getOutputCol()
        )

    def pre_op(self, dataset):
        if "SM_CONSECUTIVE_TIME_DIFFERENCE" not in dataset.columns:
            ts_window = Window.partitionBy(
                self.getOrDefault("inputCols")[1]
            ).orderBy("SM_TIMESTAMP")
            dataset = dataset.withColumn(
                "SM_PREV_TIMESTAMP",
                lag(dataset["SM_TIMESTAMP"]).over(ts_window),
            )

            dataset = dataset.withColumn(
                "SM_CONSECUTIVE_TIME_DIFFERENCE",
                when(
                    isnull(
                        dataset["SM_TIMESTAMP"].cast("long")
                        - dataset["SM_PREV_TIMESTAMP"].cast("long")
                    ),
                    0,
                ).otherwise(
                    dataset["SM_TIMESTAMP"].cast("long")
                    - dataset["SM_PREV_TIMESTAMP"].cast("long")
                ),
            )

            dataset = dataset.drop("SM_PREV_TIMESTAMP")
        return dataset

    def post_op(self, dataset):
        return dataset


class AvgTimeBtRecords(GroupbyFeature, HasTypedInputCols, HasTypedOutputCol):
    """
    Feature calculates the average time gap between all consecutive time
    entries in the group.

    Example:
    +--------+------------------------------+
    | User   | Timestamp                    |
    +========+==============================+
    | User A | 2018-01-01T00:00:04.100+0000 |
    |        | 2018-01-01T00:00:04.200+0000 |
    |        | 2018-01-01T00:00:04.250+0000 |
    |        | 2018-01-01T00:00:04.600+0000 |
    +--------+------------------------------+

    AvgTimeBtRecords = Average(Calculate difference between each consecutive
                        pair)
                     = (0.1 + 0.05 + 0.35)/3
                     = 0.16667
    """

    def __init__(
        self,
        inputCols=["SM_TIMESTAMP", "CN"],
        outputCol="AVG_TIME_BT_RECORDS",
    ):
        super(AvgTimeBtRecords, self).__init__()
        self._setDefault(
            inputCols=["SM_TIMESTAMP", "CN"],
            outputCol="AVG_TIME_BT_RECORDS",
        )
        self._set(
            inputCols=["SM_TIMESTAMP", "CN"],
            inputColsType=[TimestampType(), StringType()],
            outputCol=outputCol,
            outputColType=IntegerType(),
        )
        sch_list = []
        for x, y in zip(
            self.getOrDefault("inputCols"), self.getOrDefault("inputColsType")
        ):
            sch_list.append(StructField(x, y))
        schema = StructType(sch_list)
        self.set_input_schema(schema)

    def agg_op(self):
        return sparkround(
            sparkmean((col(self.getOrDefault("inputCols")[0]))), 5
        ).alias(self.getOutputCol())

    def pre_op(self, dataset):
        if "SM_CONSECUTIVE_TIME_DIFFERENCE" not in dataset.columns:
            ts_window = Window.partitionBy(
                self.getOrDefault("inputCols")[1]
            ).orderBy("SM_TIMESTAMP")
            dataset = dataset.withColumn(
                "SM_PREV_TIMESTAMP",
                lag(dataset["SM_TIMESTAMP"]).over(ts_window),
            )

            dataset = dataset.withColumn(
                "SM_CONSECUTIVE_TIME_DIFFERENCE",
                when(
                    isnull(
                        dataset["SM_TIMESTAMP"].cast("long")
                        - dataset["SM_PREV_TIMESTAMP"].cast("long")
                    ),
                    0,
                ).otherwise(
                    dataset["SM_TIMESTAMP"].cast("long")
                    - dataset["SM_PREV_TIMESTAMP"].cast("long")
                ),
            )

            dataset = dataset.drop("SM_PREV_TIMESTAMP")

        return dataset

    def post_op(self, dataset):
        return dataset


class UserNumOfAccountsLoginWithSameIPs(
    GroupbyFeature, HasTypedInputCol, HasTypedOutputCol
):
    """
    Feature calculates number of Usernames visited by the given IP.
    """

    def __init__(
        self,
        inputCol="SM_USERNAME",
        outputCol="USER_NUM_OF_ACCOUNTS_LOGIN_WITH_SAME_IPS",
    ):
        super(UserNumOfAccountsLoginWithSameIPs, self).__init__()
        self._setDefault(
            inputCol="SM_USERNAME",
            outputCol="USER_NUM_OF_ACCOUNTS_LOGIN_WITH_SAME_IPS",
        )
        self._set(
            inputCol="SM_USERNAME",
            inputColType=StringType(),
            outputCol=outputCol,
            outputColType=IntegerType(),
        )
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def agg_op(self):
        return sparksum(col(self.getOrDefault("inputCol"))).alias(
            self.getOutputCol()
        )

    def pre_op(self, dataset):
        if "distinct_usernames_for_ip" not in dataset.columns:
            ip_counts_df = dataset.groupBy("SM_CLIENTIP").agg(
                countDistinct("SM_USERNAME").alias("distinct_usernames_for_ip")
            )
            dataset = dataset.join(ip_counts_df, on="SM_CLIENTIP")
        return dataset

    def post_op(self, dataset):
        if "distinct_usernames_for_ip" in dataset.columns:
            dataset = dataset.drop("distinct_usernames_for_ip")
        return dataset


class StdBtRecords(GroupbyFeature, HasTypedInputCols, HasTypedOutputCol):
    """
    Feature calculates the standard deviation between consecutive time entries
    in the group.

    Example:
    +--------+------------------------------+
    | User   | Timestamp                    |
    +========+==============================+
    | User A | 2018-01-01T00:00:04.100+0000 |
    |        | 2018-01-01T00:00:04.200+0000 |
    |        | 2018-01-01T00:00:04.250+0000 |
    |        | 2018-01-01T00:00:04.600+0000 |
    +--------+------------------------------+

    StdBtRecords = Std_Dev(Calculate difference between each consecutive pair)
                 = Std_Dev(0.1, 0.05, 0.35)
                 = 0.13123
    """

    def __init__(
        self,
        inputCols=["SM_TIMESTAMP", "CN"],
        outputCol="SDV_BT_RECORDS",
    ):
        super(StdBtRecords, self).__init__()
        self._setDefault(
            inputCols=["SM_TIMESTAMP", "CN"],
            outputCol="SDV_BT_RECORDS",
        )
        self._set(
            inputCols=["SM_TIMESTAMP", "CN"],
            inputColsType=[TimestampType(), StringType()],
            outputCol=outputCol,
            outputColType=IntegerType(),
        )
        sch_list = []
        for x, y in zip(
            self.getOrDefault("inputCols"), self.getOrDefault("inputColsType")
        ):
            sch_list.append(StructField(x, y))
        schema = StructType(sch_list)
        self.set_input_schema(schema)

    def agg_op(self):
        return sparkround(
            sparkstddev((col("SM_CONSECUTIVE_TIME_DIFFERENCE"))), 5
        ).alias(self.getOutputCol())

    def pre_op(self, dataset):
        if "SM_CONSECUTIVE_TIME_DIFFERENCE" not in dataset.columns:
            ts_window = Window.partitionBy(
                self.getOrDefault("inputCols")[1]
            ).orderBy("SM_TIMESTAMP")
            dataset = dataset.withColumn(
                "SM_PREV_TIMESTAMP",
                lag(dataset["SM_TIMESTAMP"]).over(ts_window),
            )

            dataset = dataset.withColumn(
                "SM_CONSECUTIVE_TIME_DIFFERENCE",
                when(
                    isnull(
                        dataset["SM_TIMESTAMP"].cast("long")
                        - dataset["SM_PREV_TIMESTAMP"].cast("long")
                    ),
                    0,
                ).otherwise(
                    dataset["SM_TIMESTAMP"].cast("long")
                    - dataset["SM_PREV_TIMESTAMP"].cast("long")
                ),
            )

            dataset = dataset.drop("SM_PREV_TIMESTAMP")
        return dataset

    def post_op(self, dataset):
        return dataset


class UserIsUsingUnusualBrowser(
    GroupbyFeature, HasTypedInputCols, HasTypedOutputCol
):
    """
    Feature calculates 1 if the user's browser has changed between consecutive
    entries, and 0 if it remains the same in the given group.
    """

    def __init__(
        self, inputCols=["SM_AGENTNAME", "CN"], outputCol="BROWSER_LIST"
    ):
        super(UserIsUsingUnusualBrowser, self).__init__()
        self._setDefault(
            inputCols=["SM_AGENTNAME", "CN"], outputCol="BROWSER_LIST"
        )
        self._set(
            inputCols=["SM_AGENTNAME", "CN"],
            inputColsType=[StringType(), StringType()],
            outputCol=outputCol,
            outputColType=ArrayType(StringType()),
        )
        sch_list = []
        for x, y in zip(
            self.getOrDefault("inputCols"), self.getOrDefault("inputColsType")
        ):
            sch_list.append(StructField(x, y))
        schema = StructType(sch_list)
        self.set_input_schema(schema)

    def agg_op(self):
        return sort_array(
            collect_set(col(self.getOrDefault("inputCols")[0]))
        ).alias(self.getOutputCol())

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        if "USER_IS_USING_UNUSUAL_BROWSER" not in dataset.columns:
            agent_window = Window.partitionBy(
                self.getOrDefault("inputCols")[1]
            ).orderBy("window")
            dataset = dataset.withColumn(
                "SM_PREVIOUS_AGENTNAME",
                lag(dataset[self.getOrDefault("outputCol")]).over(
                    agent_window
                ),
            )
            dataset = dataset.withColumn(
                "USER_IS_USING_UNUSUAL_BROWSER",
                when(
                    (isnull("SM_PREVIOUS_AGENTNAME"))
                    | (
                        dataset[self.getOrDefault("outputCol")]
                        == dataset["SM_PREVIOUS_AGENTNAME"]
                    ),
                    0,
                ).otherwise(1),
            )
            dataset = dataset.drop(self.getOrDefault("outputCol"))
            dataset = dataset.drop("SM_PREVIOUS_AGENTNAME")
        return dataset


class UniqueCN(ArrayDistinctFeature, HasTypedInputCol):
    """
    Feature calculates a distinct list of users in inputCol(default=CN).
    """

    def __init__(self, inputCol="CN", outputCol="UNIQUE_CN"):
        super(UniqueCN, self).__init__(outputCol)
        self._setDefault(inputCol="CN", outputCol="UNIQUE_CN")
        self._set(inputCol="CN", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns column CN
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UniqueSMActions(ArrayDistinctFeature, HasTypedInputCol):
    """
    Feature calculates a distinct list of Actions in inputCol
    (default=SM_ACTION).
    """

    def __init__(self, inputCol="SM_ACTION", outputCol="UNIQUE_SM_ACTIONS"):
        super(UniqueSMActions, self).__init__(outputCol)
        self._setDefault(inputCol="SM_ACTION", outputCol="UNIQUE_SM_ACTIONS")
        self._set(inputCol="SM_ACTION", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns column SM_ACTION
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UniqueSMClientIps(ArrayDistinctFeature, HasTypedInputCol):
    """
    Feature calculates a distinct list of IPs in inputCol(default=SM_CLIENTIP).
    """

    def __init__(
        self, inputCol="SM_CLIENTIP", outputCol="UNIQUE_SM_CLIENTIPS"
    ):
        super(UniqueSMClientIps, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_CLIENTIP", outputCol="UNIQUE_SM_CLIENTIPS"
        )
        self._set(inputCol="SM_CLIENTIP", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns column SM_CLIENTIP
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UniqueSMPortals(ArrayDistinctFeature, HasTypedInputCol):
    """
    Feature calculates a distinct list of Resources in inputCol
    (default=SM_RESOURCE).
    """

    def __init__(self, inputCol="SM_RESOURCE", outputCol="UNIQUE_SM_PORTALS"):
        super(UniqueSMPortals, self).__init__(outputCol)
        self._setDefault(inputCol="SM_RESOURCE", outputCol="UNIQUE_SM_PORTALS")
        self._set(inputCol="SM_RESOURCE", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns column SM_RESOURCE
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UniqueSMTransactions(ArrayDistinctFeature, HasTypedInputCol):
    """
    Feature calculates a distinct list of Transactions in inputCol
    (default=SM_TRANSACTIONID).
    """

    def __init__(
        self, inputCol="SM_TRANSACTIONID", outputCol="UNIQUE_SM_TRANSACTIONS"
    ):
        super(UniqueSMTransactions, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_TRANSACTIONID", outputCol="UNIQUE_SM_TRANSACTIONS"
        )
        self._set(inputCol="SM_TRANSACTIONID", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns column SM_TRANSACTIONID
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UniqueSMSessionIds(ArrayDistinctFeature, HasTypedInputCol):
    """
    Feature calculates a distinct list of Session IDs in inputCol
    (default=SM_SESSIONID).
    """

    def __init__(
        self, inputCol="SM_SESSIONID", outputCol="UNIQUE_SM_SESSION_IDS"
    ):
        super(UniqueSMSessionIds, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_SESSIONID", outputCol="UNIQUE_SM_SESSION_IDS"
        )
        self._set(inputCol="SM_SESSIONID", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns column SM_SESSIONID
        :rtype: pyspark.sql.Column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UniqueUserOU(ArrayRemoveFeature, HasTypedInputCol):
    """
    Feature calculates a distinct list of User OUs defined by
    entries containing "rep" and ending in "/" in inputCol
    (default=SM_USERNAME).
    """

    def __init__(self, inputCol="SM_USERNAME", outputCol="UNIQUE_USER_OU"):
        super(UniqueUserOU, self).__init__(outputCol)
        self._setDefault(inputCol="SM_USERNAME", outputCol="UNIQUE_USER_OU")
        self._set(inputCol="SM_USERNAME", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns regex-modified list of strings from SM_USERNAME
        :rtype: ArrayType(StringType())
        """
        return collect_list(
            regexp_extract(self.getOrDefault("inputCol"), r"ou=(,*?),", 0)
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UniquePortalRep(ArrayRemoveFeature, HasTypedInputCol):
    """
    Feature calculates a distinct list of Reps defined by
    entries containing "rep" and ending in "/" in inputCol(default=SM_RESOURCE)
    """

    def __init__(self, inputCol="SM_RESOURCE", outputCol="UNIQUE_PORTAL_REP"):
        super(UniquePortalRep, self).__init__(outputCol)
        self._setDefault(inputCol="SM_RESOURCE", outputCol="UNIQUE_PORTAL_REP")
        self._set(inputCol="SM_RESOURCE", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns regex-modified list of strings from SM_RESOURCE
        :rtype: ArrayType(StringType())
        """
        return collect_list(
            regexp_extract(self.getOrDefault("inputCol"), r"(rep.*?)/", 0)
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UniqueUserApps(ArrayRemoveFeature, HasTypedInputCol):
    """
    Feature calculates a distinct list of Apps visited by the user
    defined by entries containing "/" and ending in "/" in inputCol
    (default=SM_RESOURCE)
    """

    def __init__(self, inputCol="SM_RESOURCE", outputCol="UNIQUE_USER_APPS"):
        super(UniqueUserApps, self).__init__(outputCol)
        self._setDefault(inputCol="SM_RESOURCE", outputCol="UNIQUE_USER_APPS")
        self._set(inputCol="SM_RESOURCE", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns regex-modified list of strings from SM_RESOURCE
        :rtype: ArrayType(StringType())
        """
        return collect_list(
            regexp_extract(self.getOrDefault("inputCol"), r"/(.*?)/", 0)
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountUniqueOU(SizeArrayRemoveFeature, HasTypedInputCol):
    """
    Feature calculates number of distinct User OUs defined by
    entries containing "ou=" and ending in "," in inputCol(default=SM_USERNAME)
    """

    def __init__(self, inputCol="SM_USERNAME", outputCol="COUNT_UNIQUE_OU"):
        super(CountUniqueOU, self).__init__(outputCol)
        self._setDefault(inputCol="SM_USERNAME", outputCol="COUNT_UNIQUE_OU")
        self._set(inputCol="SM_USERNAME", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns regex-modified list of strings from SM_USERNAME
        :rtype: ArrayType(StringType())
        """
        return collect_list(
            regexp_extract(self.getOrDefault("inputCol"), r"ou=(,*?),", 0)
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountUniqueRep(SizeArrayRemoveFeature, HasTypedInputCol):
    """
    Feature calculates number of distinct Reps defined by
    entries containing "rep" and ending in "/" in inputCol(default=SM_RESOURCE)
    """

    def __init__(self, inputCol="SM_RESOURCE", outputCol="COUNT_UNIQUE_REP"):
        super(CountUniqueRep, self).__init__(outputCol)
        self._setDefault(inputCol="SM_RESOURCE", outputCol="COUNT_UNIQUE_REP")
        self._set(inputCol="SM_RESOURCE", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns regex-modified list of strings from SM_RESOURCE
        :rtype: ArrayType(StringType())
        """
        return array_distinct(
            collect_list(
                regexp_extract(self.getOrDefault("inputCol"), r"(rep.*?)/", 0)
            )
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountUniqueUserApps(SizeArrayRemoveFeature, HasTypedInputCol):
    """
    Feature calculates number of distinct Apps visited by the user
    defined by entries containing "/" and ending in "/" in inputCol
    (default=SM_RESOURCE).
    """

    def __init__(
        self, inputCol="SM_RESOURCE", outputCol="COUNT_UNIQUE_USER_APPS"
    ):
        super(CountUniqueUserApps, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_RESOURCE", outputCol="COUNT_UNIQUE_USER_APPS"
        )
        self._set(inputCol="SM_RESOURCE", inputColType=StringType())
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def array_clause(self):
        """
        :return: Returns regex-modified list of strings from SM_RESOURCE
        :rtype: ArrayType(StringType())
        """
        return array_distinct(
            collect_list(
                regexp_extract(self.getOrDefault("inputCol"), r"/(.*?)/", 0)
            )
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class FlattenerFeature(GroupbyFeature, HasTypedInputCol, HasTypedOutputCol):
    """
    Feature used to calculate a set of Strings from inputCol, where number
    of items in the set is limited by max_list_count.
    """

    max_list_count = Param(
        Params._dummy(),
        "max_list_count",
        "Maximum count of Strings allowed in the set.",
        typeConverter=TypeConverters.toInt,
    )

    def __init__(self, inputCol, outputCol, max_list_count=3):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType
        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        :param max_resource_count: Maximum count of Strings allowed in the set.
        :type max_list_count: IntegerType
        """
        super(FlattenerFeature, self).__init__()
        self._set(
            inputCol=inputCol,
            inputColType=StringType(),
            outputCol=outputCol,
            outputColType=IntegerType(),
            max_list_count=max_list_count,
        )
        schema = StructType(
            [
                StructField(
                    self.getOrDefault("inputCol"),
                    self.getOrDefault("inputColType"),
                )
            ]
        )
        self.set_input_schema(schema)

    def agg_op(self):
        return sparkslice(
            collect_set(self.getOrDefault("inputCol")),
            1,
            self.getOrDefault("max_list_count"),
        ).alias(self.getOutputCol())

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset
