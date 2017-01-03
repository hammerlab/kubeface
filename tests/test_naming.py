from numpy import testing

from kubeface import naming


def test_basics():
    job = naming.JOB.make_string(cache_key="foo", randomness="123")
    print(job)
    testing.assert_equal(
        naming.JOB.make_string(naming.JOB.make_tuple(job)),
        job)
    testing.assert_equal(
        naming.JOB.prefix(cache_key=["foo"]),
        "foo::")
    testing.assert_equal(
        naming.JOB.prefix(cache_key=["foo", "fob"]),
        "fo")

    job_status = naming.JOB_STATUS_PAGE.make_string(
        format="json", status="active", job_name="foobar")
    testing.assert_equal(
        naming.JOB_STATUS_PAGE.make_string(
            naming.JOB_STATUS_PAGE.make_tuple(job_status)),
        job_status)
    testing.assert_equal(
        set(naming.JOB_STATUS_PAGE.prefixes(
            max_prefixes=2,
            status=["active", "done"])),
        set(["done::", "active::"]))
    testing.assert_equal(
        set(naming.JOB_STATUS_PAGE.prefixes(
            max_prefixes=4,
            status=["active", "done"],
            format=["html", "json"])),
        set([
            "done::html::",
            "active::html::",
            "done::json::",
            "active::json::"]))
    testing.assert_equal(
        set(naming.JOB_STATUS_PAGE.prefixes(
            max_prefixes=4)),
        set([
            "done::html::",
            "active::html::",
            "done::json::",
            "active::json::"]))
    testing.assert_equal(
        set(naming.JOB_STATUS_PAGE.prefixes(
            max_prefixes=4,
            job_name=["foo1", "foo2"])),
        set([
            "done::html::foo",
            "active::html::foo",
            "done::json::foo",
            "active::json::foo"]))
    testing.assert_equal(
        set(naming.JOB_STATUS_PAGE.prefixes(
            max_prefixes=9,
            job_name=["foo1", "foo2"])),
        set(
            [
                "done::html::foo1.html",
                "active::html::foo1.html",
                "done::json::foo1.json",
                "active::json::foo1.json",
                "done::html::foo2.html",
                "active::html::foo2.html",
                "done::json::foo2.json",
                "active::json::foo2.json",
            ]
        ))
