from pyspark.sql.functions import col, when
from pyspark.sql.types import IntegerType, StringType, LongType
from utils import HasTypedInputCol, HasTypedInputCols
from base import CounterFeature, ArrayDistinctFeature


class CountAuthAccept(CounterFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AUTH_ACCEPT"):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountAuthAccept, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_AUTH_ACCEPT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 1.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 1, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAuthReject(CounterFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AUTH_REJECT"):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountAuthReject, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_AUTH_REJECT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 2.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 2, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAdminAttempt(CounterFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_ATTEMPT"):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountAdminAttempt, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_ATTEMPT"
        )
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 3.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 3, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAuthChallenge(CounterFeature, HasTypedInputCol):
    def __init__(
        self, inputCol="SM_EVENTID", outputCol="COUNT_AUTH_CHALLENGE"
    ):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountAuthChallenge, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_EVENTID", outputCol="COUNT_AUTH_CHALLENGE"
        )
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 4.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 4, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAZAccept(CounterFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AZ_ACCEPT"):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountAZAccept, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_AZ_ACCEPT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 5.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 5, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAZReject(CounterFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AZ_REJECT"):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountAZReject, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_AZ_REJECT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 6.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 6, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAdminLogin(CounterFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_LOGIN"):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountAdminLogin, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_LOGIN")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 7.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 7, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAdminLogout(CounterFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_LOGOUT"):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountAdminLogout, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_LOGOUT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 8.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 8, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAdminReject(CounterFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_REJECT"):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountAdminReject, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_ADMIN_REJECT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 9.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 9, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountAuthLogout(CounterFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_AUTH_LOGOUT"):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountAuthLogout, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_AUTH_LOGOUT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 10.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 10, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountValidateAccept(CounterFeature, HasTypedInputCol):
    def __init__(
        self, inputCol="SM_EVENTID", outputCol="COUNT_VALIDATE_ACCEPT"
    ):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountValidateAccept, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_EVENTID", outputCol="COUNT_VALIDATE_ACCEPT"
        )
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 11.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 11, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountValidateReject(CounterFeature, HasTypedInputCol):
    def __init__(
        self, inputCol="SM_EVENTID", outputCol="COUNT_VALIDATE_REJECT"
    ):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountValidateReject, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_EVENTID", outputCol="COUNT_VALIDATE_REJECT"
        )
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 12.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 12, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountVisit(CounterFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_VISIT"):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountVisit, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_VISIT")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

        :return: Returns True if EVENTID == 13.
        :rtype: BooleanType
        """
        return when(col(self.getOrDefault("inputCol")) == 13, True)

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class CountFailed(CounterFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_EVENTID", outputCol="COUNT_FAILED"):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(CountFailed, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="COUNT_FAILED")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

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
    Counter for occurences of "ams" or "AMS" in SM_USERNAME or SM_RESOURCE
    """

    def __init__(
        self, inputCol=["SM_USERNAME", "SM_RESOURCE"], outputCol="COUNT_OU_AMS"
    ):
        """
        :param inputCol: Columns to search through, SM_USERNAME and SM_RESOURCE
        by default
        :param outputCol: Column to write the count to
        :type inputCol: list of StringTypes
        :type outputCol: StringType
        """
        super(CountOUAms, self).__init__(outputCol)
        self._setDefault(
            inputCols=["SM_USERNAME", "SM_RESOURCE"], outputCol="COUNT_OU_AMS"
        )
        self._set(
            inputCols=["SM_USERNAME", "SM_RESOURCE"],
            inputColsType=[StringType(), StringType()],
        )

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

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
    Counter for occurences of "cra-cp" in SM_USERNAME
    """

    def __init__(self, inputCol="SM_USERNAME", outputCol="COUNT_OU_CMS"):
        super(CountOUCms, self).__init__(outputCol)
        self._setDefault(inputCol="SM_USERNAME", outputCol="COUNT_OU_AMS")
        self._set(inputCol="SM_USERNAME", inputColType=StringType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

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
    Counter for occurences of "GET" in SM_ACTION
    """

    def __init__(self, inputCol="SM_ACTION", outputCol="COUNT_GET"):
        super(CountGet, self).__init__(outputCol)
        self._setDefault(inputCol="SM_ACTION", outputCol="COUNT_GET")
        self._set(inputCol="SM_ACTION", inputColType=StringType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

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
    Counter for occurences of "POST" in SM_ACTION
    """

    def __init__(self, inputCol="SM_ACTION", outputCol="COUNT_POST"):
        super(CountPost, self).__init__(outputCol)
        self._setDefault(inputCol="SM_ACTION", outputCol="COUNT_POST")
        self._set(inputCol="SM_ACTION", inputColType=StringType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

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
    Counter for occurences of "GET" or "POST" in SM_ACTION
    """

    def __init__(self, inputCol="SM_ACTION", outputCol="COUNT_HTTP_METHOD"):
        super(CountHTTPMethod, self).__init__(outputCol)
        self._setDefault(inputCol="SM_ACTION", outputCol="COUNT_HTTP_METHOD")
        self._set(inputCol="SM_ACTION", inputColType=StringType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

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
    Counter for occurences of "ou=Identity" in SM_USERNAME
    """

    def __init__(self, inputCol="SM_USERNAME", outputCol="COUNT_OU_IDENTITY"):
        super(CountOUIdentity, self).__init__(outputCol)
        self._setDefault(inputCol="SM_USERNAME", outputCol="COUNT_OU_IDENTITY")
        self._set(inputCol="SM_USERNAME", inputColType=StringType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

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
    Counter for occurences of "ou=Credential" in SM_USERNAME
    """

    def __init__(self, inputCol="SM_USERNAME", outputCol="COUNT_OU_CRED"):
        super(CountOUCred, self).__init__(outputCol)
        self._setDefault(inputCol="SM_USERNAME", outputCol="COUNT_OU_CRED")
        self._set(inputCol="SM_USERNAME", inputColType=StringType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

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
    Counter for occurences of "ou=SecureKey" in SM_USERNAME
    """

    def __init__(self, inputCol="SM_USERNAME", outputCol="COUNT_OU_SECUREKEY"):
        super(CountOUSecurekey, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_USERNAME", outputCol="COUNT_OU_SECUREKEY"
        )
        self._set(inputCol="SM_USERNAME", inputColType=StringType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

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
    Counter for occurences of "mima" in SM_RESOURCE
    """

    def __init__(self, inputCol="SM_RESOURCE", outputCol="COUNT_PORTAL_MYA"):
        super(CountPortalMya, self).__init__(outputCol)
        self._setDefault(inputCol="SM_RESOURCE", outputCol="COUNT_PORTAL_MYA")
        self._set(inputCol="SM_RESOURCE", inputColType=StringType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

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
    Counter for occurences of "myba" in SM_RESOURCE
    """

    def __init__(self, inputCol="SM_RESOURCE", outputCol="COUNT_PORTAL_MYBA"):
        super(CountPortalMyba, self).__init__(outputCol)
        self._setDefault(inputCol="SM_RESOURCE", outputCol="COUNT_PORTAL_MYBA")
        self._set(inputCol="SM_RESOURCE", inputColType=StringType())

    def count_clause(self):
        """
        Implementation of the base logic of required count feature.

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
    def __init__(self, inputCol="CRA_SEQ", outputCol="COUNT_RECORDS"):
        super(CountRecords, self).__init__(outputCol)
        self._setDefault(inputCol="CRA_SEQ", outputCol="COUNT_RECORDS")
        self._set(inputCol="CRA_SEQ", inputColType=LongType())

    def count_clause(self):
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UserLoginAttempts(CounterFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_EVENTID", outputCol="UserLoginAttempts"):
        super(UserLoginAttempts, self).__init__(outputCol)
        self._setDefault(inputCol="SM_EVENTID", outputCol="UserLoginAttempts")
        self._set(inputCol="SM_EVENTID", inputColType=IntegerType())

    def count_clause(self):
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
    def __init__(
        self, inputCol="SM_RESOURCE", outputCol="UserNumOfPasswordChange"
    ):
        super(UserNumOfPasswordChange, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_RESOURCE", outputCol="UserNumOfPasswordChange"
        )
        self._set(inputCol="SM_RESOURCE", inputColType=StringType())

    def count_clause(self):
        return when(
            ((col(self.getOrDefault("inputCol")).contains("changePassword"))),
            True,
        )

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UniqueSMActions(ArrayDistinctFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_ACTION", outputCol="UNIQUE_SM_ACTIONS"):
        super(UniqueSMActions, self).__init__(outputCol)
        self._setDefault(inputCol="SM_ACTION", outputCol="UNIQUE_SM_ACTIONS")
        self._set(inputCol="SM_ACTION", inputColType=ArrayType(StringType()))

    def array_clause(self):
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UniqueSMClientIps(ArrayDistinctFeature, HasTypedInputCol):
    def __init__(
        self, inputCol="SM_CLIENTIP", outputCol="UNIQUE_SM_CLIENTIPS"
    ):
        super(UniqueSMClientIps, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_CLIENTIP", outputCol="UNIQUE_SM_CLIENTIPS"
        )
        self._set(inputCol="SM_CLIENTIP", inputColType=ArrayType(StringType()))

    def array_clause(self):
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UniqueSMPortals(ArrayDistinctFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_RESOURCE", outputCol="UNIQUE_SM_PORTALS"):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(UniqueSMPortals, self).__init__(outputCol)
        self._setDefault(inputCol="SM_RESOURCE", outputCol="UNIQUE_SM_PORTALS")
        self._set(inputCol="SM_RESOURCE", inputColType=ArrayType(StringType()))

    def array_clause(self):
        """
        Implementation of the base logic of required array distinct feature.

        :return: Column
        :rtype: pyspark column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class UniqueSMTransactions(ArrayDistinctFeature, HasTypedInputCol):
    
    def __init__(
        self, inputCol="SM_TRANSACTIONID", outputCol="UNIQUE_SM_TRANSACTIONS"
    ):
        """
        :param inputCol: Name for the input Column of the feature.
        :type inputCol: StringType

        :param outputCol: Name for the output Column of the feature.
        :type outputCol: StringType
        """
        super(UniqueSMTransactions, self).__init__(outputCol)
        self._setDefault(
            inputCol="SM_TRANSACTIONID", outputCol="UNIQUE_SM_TRANSACTIONS"
        )
        self._set(
            inputCol="SM_TRANSACTIONID", inputColType=ArrayType(StringType())
        )

    def array_clause(self):
        """
        Implementation of the base logic of required array distinct feature.

        :return: Column
        :rtype: pyspark column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset


class SMSessionIds(ArrayDistinctFeature, HasTypedInputCol):
    def __init__(self, inputCol="SM_SESSIONID", outputCol="SM_SESSION_IDS"):
        super(SMSessionIds, self).__init__(outputCol)
        self._setDefault(inputCol="SM_SESSIONID", outputCol="SM_SESSION_IDS")
        self._set(
            inputCol="SM_SESSIONID", inputColType=ArrayType(StringType())
        )

    def array_clause(self):
        """
        Implementation of the base logic of required array distinct feature.

        :return: Column
        :rtype: pyspark column
        """
        return col(self.getOrDefault("inputCol"))

    def pre_op(self, dataset):
        return dataset

    def post_op(self, dataset):
        return dataset
