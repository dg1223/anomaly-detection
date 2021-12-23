from pyspark.sql.session import SparkSession
from pytest import raises

import src.caaswx.spark.features as ft
from src.caaswx.spark.utils import load_test_data

spark = SparkSession.builder.getOrCreate()

# Get data
ufg_df = load_test_data(
    "data",
    "parquet_data",
    "user_feature_generator_tests",
    "data.parquet",
)


def test_unique_cn():

    result_df = ft.UniqueCN().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == [
        "cn=0935dd13-5b8d-4dc7-98f4-e3f16ae41897"
    ]

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.UniqueCN(inputCol="testin").get_transformer(["CN"]).transform(
            ufg_df
        )

    test_df = ufg_df.withColumn("testin", ufg_df["CN"]).drop("CN")
    result_df = (
        ft.UniqueCN(inputCol="testin")
        .get_transformer(["testin"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == [
        "cn=0935dd13-5b8d-4dc7-98f4-e3f16ae41897"
    ]

    result_df = (
        ft.UniqueCN(outputCol="testout")
        .get_transformer(["CN"])
        .transform(ufg_df)
    )

    """
    Test for output column name change functionality
    """
    assert "testout" in result_df.columns

    """
    Test for correct number of rows in result dataframe with specified output
    column
    """
    assert result_df.count() == 1

    """
    Test for feature functionality with specified output column
    """
    assert result_df.collect()[0][1] == [
        "cn=0935dd13-5b8d-4dc7-98f4-e3f16ae41897"
    ]


def test_unique_sm_actions():

    result_df = ft.UniqueSMActions().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == ["GET"]

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.UniqueSMActions(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_ACTION"]).drop(
        "SM_ACTION"
    )
    result_df = (
        ft.UniqueSMActions(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == ["GET"]

    result_df = (
        ft.UniqueSMActions(outputCol="testout")
        .get_transformer(["CN"])
        .transform(ufg_df)
    )

    """
    Test for output column name change functionality
    """
    assert "testout" in result_df.columns

    """
    Test for correct number of rows in result dataframe with specified output
    column
    """
    assert result_df.count() == 1

    """
    Test for feature functionality with specified output column
    """
    assert result_df.collect()[0][1] == ["GET"]


def test_unique_sm_clientips():

    result_df = (
        ft.UniqueSMClientIps().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == [
        "dc59950862e98863d5ed858fffa225b3cf638c5ba89da97c0789830bcbe03f46",
        "b7592207b43888dd379bb638211268f2ef9fa198fdfe714bca4994f6bc308103",
    ]

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.UniqueSMClientIps(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_CLIENTIP"]).drop(
        "SM_CLIENTIP"
    )
    result_df = (
        ft.UniqueSMClientIps(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == [
        "dc59950862e98863d5ed858fffa225b3cf638c5ba89da97c0789830bcbe03f46",
        "b7592207b43888dd379bb638211268f2ef9fa198fdfe714bca4994f6bc308103",
    ]

    result_df = (
        ft.UniqueSMClientIps(outputCol="testout")
        .get_transformer(["CN"])
        .transform(ufg_df)
    )

    """
    Test for output column name change functionality
    """
    assert "testout" in result_df.columns

    """
    Test for correct number of rows in result dataframe with specified output
    column
    """
    assert result_df.count() == 1

    """
    Test for feature functionality with specified output column
    """
    assert result_df.collect()[0][1] == [
        "dc59950862e98863d5ed858fffa225b3cf638c5ba89da97c0789830bcbe03f46",
        "b7592207b43888dd379bb638211268f2ef9fa198fdfe714bca4994f6bc308103",
    ]


def test_unique_sm_portals():

    result_df = ft.UniqueSMPortals().get_transformer(["CN"]).transform(ufg_df)
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == [
        "/smiavalidationrealm",
        "/smauthenticationrealm",
        "/cmsws/redirectjsp/redirect.jsp?SAMLRequest"
        "=fVA9b4MwEN3zK5B38AeEEAuQImWJ1C5t1aFLdTaGUIFNfUbtz69Dh7ZLT6cbnt69u"
        "%2Ffq0xqu9sG8rwZDco5jtBBGZxtyDWFBSameMcXhLdMeUvA6G3Sm4YZ"
        "%2BIF1WNY2aIsyTQHQkuZwb8toVvery6pDv96U6CFDdsejLouy04qbqC54bEamIq7lYDG"
        "BDQwTjVcpEyvgTYzI2L19I8mw8bs%2BIjJHkc54sNmT1VjrAEaWF2aAMWj6e7u9k5MjFu"
        "%2BC0m0i7S2LVFrncDvnvbRmB%2FxUA0fhbAqSd3DDaDDb%2F8Nd%2FTX%2BU211Nf8fY"
        "fgE%3D&RelayState=c7e394348bad44528482cdc8d8441873d318e3f6&SigAlg=htt"
        "p%3A%2F%2Fwww.w3.org%2F2001%2F04%2Fxmldsig-more%23rsa-sha256&Signatur"
        "e=dAZ0GVvTq7gVtLYt7E7WO47ATGQRIT8SPmXP8ioiCXJA2Uw%2B%2Fa9h9Ua7mOahPcF"
        "ugRSeJiVVCmJgWn66bpdderv6S64jN9FdpGjPw2OAjMr61NEyma1rxfBx1nVqp6EFR1k1"
        "vnwZKyJQI4XvBD817IVwkwoeoYl0eItlmOvx07rqzRTJWIzscKqjUw5w%2Bera4PYDHQm"
        "ZK9Qqd3uQri2VGP%2BjcydXK3WAOHweLWLS8sRKJk8rho97QK2dO18XK70IJCBrhM9Bwj"
        "4QroEQFDnU7uIEQEm%2FiE8TbzHIN3PFg%2FQV6KrFrm1bMS9OWl3jnLWb25%2B2hzL6D"
        "IuZ%2BDT9hHK2Bg%3D%3D&SMPORTALURL=https%3A%2F%2Fcms-sgj.cra-arc.gc.ca"
        "%2Fcmsws%2Fpublic%2Fsaml2sso",
        "/gol-ged/awsc/amss/enrol/loginCheck",
        "/cms-sgj/prot/postLogin",
        "/gol-ged/awsc/amss/enrol/redirectx/gol-ged/pzaz/tppm/prot/Start.do",
    ]

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.UniqueSMPortals(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_RESOURCE"]).drop(
        "SM_RESOURCE"
    )
    result_df = (
        ft.UniqueSMPortals(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == [
        "/smiavalidationrealm",
        "/smauthenticationrealm",
        "/cmsws/redirectjsp/redirect.jsp?SAMLRequest"
        "=fVA9b4MwEN3zK5B38AeEEAuQImWJ1C5t1aFLdTaGUIFNfUbtz69Dh7ZLT6cbnt69u"
        "%2Ffq0xqu9sG8rwZDco5jtBBGZxtyDWFBSameMcXhLdMeUvA6G3Sm4YZ"
        "%2BIF1WNY2aIsyTQHQkuZwb8toVvery6pDv96U6CFDdsejLouy04qbqC54bEamIq7lYDG"
        "BDQwTjVcpEyvgTYzI2L19I8mw8bs%2BIjJHkc54sNmT1VjrAEaWF2aAMWj6e7u9k5MjFu"
        "%2BC0m0i7S2LVFrncDvnvbRmB%2FxUA0fhbAqSd3DDaDDb%2F8Nd%2FTX%2BU211Nf8fY"
        "fgE%3D&RelayState=c7e394348bad44528482cdc8d8441873d318e3f6&SigAlg=htt"
        "p%3A%2F%2Fwww.w3.org%2F2001%2F04%2Fxmldsig-more%23rsa-sha256&Signatur"
        "e=dAZ0GVvTq7gVtLYt7E7WO47ATGQRIT8SPmXP8ioiCXJA2Uw%2B%2Fa9h9Ua7mOahPcF"
        "ugRSeJiVVCmJgWn66bpdderv6S64jN9FdpGjPw2OAjMr61NEyma1rxfBx1nVqp6EFR1k1"
        "vnwZKyJQI4XvBD817IVwkwoeoYl0eItlmOvx07rqzRTJWIzscKqjUw5w%2Bera4PYDHQm"
        "ZK9Qqd3uQri2VGP%2BjcydXK3WAOHweLWLS8sRKJk8rho97QK2dO18XK70IJCBrhM9Bwj"
        "4QroEQFDnU7uIEQEm%2FiE8TbzHIN3PFg%2FQV6KrFrm1bMS9OWl3jnLWb25%2B2hzL6D"
        "IuZ%2BDT9hHK2Bg%3D%3D&SMPORTALURL=https%3A%2F%2Fcms-sgj.cra-arc.gc.ca"
        "%2Fcmsws%2Fpublic%2Fsaml2sso",
        "/gol-ged/awsc/amss/enrol/loginCheck",
        "/cms-sgj/prot/postLogin",
        "/gol-ged/awsc/amss/enrol/redirectx/gol-ged/pzaz/tppm/prot/Start.do",
    ]

    result_df = (
        ft.UniqueSMPortals(outputCol="testout")
        .get_transformer(["CN"])
        .transform(ufg_df)
    )

    """
    Test for output column name change functionality
    """
    assert "testout" in result_df.columns

    """
    Test for correct number of rows in result dataframe with specified output
    column
    """
    assert result_df.count() == 1

    """
    Test for feature functionality with specified output column
    """
    assert result_df.collect()[0][1] == [
        "/smiavalidationrealm",
        "/smauthenticationrealm",
        "/cmsws/redirectjsp/redirect.jsp?SAMLRequest"
        "=fVA9b4MwEN3zK5B38AeEEAuQImWJ1C5t1aFLdTaGUIFNfUbtz69Dh7ZLT6cbnt69u"
        "%2Ffq0xqu9sG8rwZDco5jtBBGZxtyDWFBSameMcXhLdMeUvA6G3Sm4YZ"
        "%2BIF1WNY2aIsyTQHQkuZwb8toVvery6pDv96U6CFDdsejLouy04qbqC54bEamIq7lYDG"
        "BDQwTjVcpEyvgTYzI2L19I8mw8bs%2BIjJHkc54sNmT1VjrAEaWF2aAMWj6e7u9k5MjFu"
        "%2BC0m0i7S2LVFrncDvnvbRmB%2FxUA0fhbAqSd3DDaDDb%2F8Nd%2FTX%2BU211Nf8fY"
        "fgE%3D&RelayState=c7e394348bad44528482cdc8d8441873d318e3f6&SigAlg=htt"
        "p%3A%2F%2Fwww.w3.org%2F2001%2F04%2Fxmldsig-more%23rsa-sha256&Signatur"
        "e=dAZ0GVvTq7gVtLYt7E7WO47ATGQRIT8SPmXP8ioiCXJA2Uw%2B%2Fa9h9Ua7mOahPcF"
        "ugRSeJiVVCmJgWn66bpdderv6S64jN9FdpGjPw2OAjMr61NEyma1rxfBx1nVqp6EFR1k1"
        "vnwZKyJQI4XvBD817IVwkwoeoYl0eItlmOvx07rqzRTJWIzscKqjUw5w%2Bera4PYDHQm"
        "ZK9Qqd3uQri2VGP%2BjcydXK3WAOHweLWLS8sRKJk8rho97QK2dO18XK70IJCBrhM9Bwj"
        "4QroEQFDnU7uIEQEm%2FiE8TbzHIN3PFg%2FQV6KrFrm1bMS9OWl3jnLWb25%2B2hzL6D"
        "IuZ%2BDT9hHK2Bg%3D%3D&SMPORTALURL=https%3A%2F%2Fcms-sgj.cra-arc.gc.ca"
        "%2Fcmsws%2Fpublic%2Fsaml2sso",
        "/gol-ged/awsc/amss/enrol/loginCheck",
        "/cms-sgj/prot/postLogin",
        "/gol-ged/awsc/amss/enrol/redirectx/gol-ged/pzaz/tppm/prot/Start.do",
    ]


def test_unique_sm_transactions():

    result_df = (
        ft.UniqueSMTransactions().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == [
        "idletime=1200;maxtime=43200;authlevel=50;",
        "idletime=1200;maxtime=43200;authlevel=100;",
        "idletime=1200;maxtime=43200;authlevel=5;",
    ]

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.UniqueSMTransactions(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_TRANSACTIONID"]).drop(
        "SM_TRANSACTIONID"
    )
    result_df = (
        ft.UniqueSMTransactions(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == [
        "idletime=1200;maxtime=43200;authlevel=50;",
        "idletime=1200;maxtime=43200;authlevel=100;",
        "idletime=1200;maxtime=43200;authlevel=5;",
    ]

    result_df = (
        ft.UniqueSMTransactions(outputCol="testout")
        .get_transformer(["CN"])
        .transform(ufg_df)
    )

    """
    Test for output column name change functionality
    """
    assert "testout" in result_df.columns

    """
    Test for correct number of rows in result dataframe with specified output
    column
    """
    assert result_df.count() == 1

    """
    Test for feature functionality with specified output column
    """
    assert result_df.collect()[0][1] == [
        "idletime=1200;maxtime=43200;authlevel=50;",
        "idletime=1200;maxtime=43200;authlevel=100;",
        "idletime=1200;maxtime=43200;authlevel=5;",
    ]


def test_unique_sm_sessionids():

    result_df = (
        ft.UniqueSMSessionIds().get_transformer(["CN"]).transform(ufg_df)
    )
    """
    Test for default feature functionality
    """
    assert result_df.collect()[0][1] == [
        "gxCoecnuWqQzPv1enay6qu8RwVM=",
        "xnE/KzaTA47fZTwRBEfJSsD8mjY=",
        "GQ/HRbEPwe9laRroADbqJ/DwlhU=",
    ]

    """
    Test for valid input column name (if name exists in input dataframe)
    """
    with raises(ValueError):
        ft.UniqueSMSessionIds(inputCol="testin").get_transformer(
            ["CN"]
        ).transform(ufg_df)

    test_df = ufg_df.withColumn("testin", ufg_df["SM_SESSIONID"]).drop(
        "SM_SESSIONID"
    )
    result_df = (
        ft.UniqueSMSessionIds(inputCol="testin")
        .get_transformer(["CN"])
        .transform(test_df)
    )
    """
    Test for input column name change functionality
    """
    assert result_df.collect()[0][1] == [
        "gxCoecnuWqQzPv1enay6qu8RwVM=",
        "xnE/KzaTA47fZTwRBEfJSsD8mjY=",
        "GQ/HRbEPwe9laRroADbqJ/DwlhU=",
    ]

    result_df = (
        ft.UniqueSMSessionIds(outputCol="testout")
        .get_transformer(["CN"])
        .transform(ufg_df)
    )

    """
    Test for output column name change functionality
    """
    assert "testout" in result_df.columns

    """
    Test for correct number of rows in result dataframe with specified output
    column
    """
    assert result_df.count() == 1

    """
    Test for feature functionality with specified output column
    """
    assert result_df.collect()[0][1] == [
        "gxCoecnuWqQzPv1enay6qu8RwVM=",
        "xnE/KzaTA47fZTwRBEfJSsD8mjY=",
        "GQ/HRbEPwe9laRroADbqJ/DwlhU=",
    ]
