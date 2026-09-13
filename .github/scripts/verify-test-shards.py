#!/usr/bin/env python3
"""Verify that core/pom.xml still supports the nightly Windows shard arguments.

The nightly Windows legs in .github/workflows/maven-integration-tests-pipeline.yml split
core's test phase across jobs, because one invocation no longer fits inside GitHub's
unraisable 360-minute cap on a hosted-runner job. A shard selects its share two ways, and
both fail silently when core/pom.xml drifts away from the workflow:

  * Deactivation. A shard passes -P '!<profile-id>' for the compliance profiles it does not
    own. Maven only warns when a -P deactivation names a profile that does not exist, so a
    renamed profile turns the deactivation into a no-op and the shard runs suites another
    shard already covers.
  * Unbinding. The shard-skip-core-unit-tests profile rebinds executions to phase `none`.
    Execution merging is by id, so a renamed execution makes the profile's entry merge as a
    new goal-less execution instead of unbinding the real one. That case was live for the
    vmlens plugin until review caught it: unbinding surefire alone left core's *MTTest
    classes running in all four shards.

Neither failure loses coverage — both run tests more often than intended — but both waste a
slow runner and neither is visible without reading a warning out of a multi-hour log. This
script asserts the structure the workflow depends on so drift fails loudly instead.
"""

import argparse
import pathlib
import sys
import xml.etree.ElementTree as ET

NS = {"m": "http://maven.apache.org/POM/4.0.0"}
# Maven's default plugin groupId, applied when a <plugin> omits <groupId>.
DEFAULT_PLUGIN_GROUP = "org.apache.maven.plugins"

SHARD_PROFILE = "shard-skip-core-unit-tests"
ACTIVATION_PROPERTY = "youtrackdb.shard.skipCoreUnitTests"
# Profile ids the shard arguments deactivate with -P '!<id>'.
DEACTIVATED_PROFILES = (
    "gremlin-compliance-process-suites",
    "gremlin-compliance-structure-suites",
)


def workflow_error(message):
    """Emit a GitHub Actions error annotation."""
    message = str(message).replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::error::{message}")


def find_profile(root, profile_id):
    """Return the <profile> element with this id, or None."""
    for profile in root.iterfind("./m:profiles/m:profile", NS):
        if profile.findtext("./m:id", namespaces=NS) == profile_id:
            return profile
    return None


def plugin_key(plugin):
    """Return (groupId, artifactId), defaulting the group the way Maven does."""
    group = plugin.findtext("./m:groupId", default=DEFAULT_PLUGIN_GROUP, namespaces=NS)
    return group, plugin.findtext("./m:artifactId", namespaces=NS)


def build_execution_ids(root, key):
    """Return the execution ids this plugin declares in the base <build>, or None.

    None means the base <build> declares no such plugin at all, which is a different
    failure from declaring it without the execution.
    """
    for plugin in root.iterfind("./m:build/m:plugins/m:plugin", NS):
        if plugin_key(plugin) == key:
            return {
                execution.findtext("./m:id", namespaces=NS)
                for execution in plugin.iterfind("./m:executions/m:execution", NS)
            }
    return None


def check_activation(shard, errors):
    """The profile must activate on the shard property, and on the value `true`."""
    prop = shard.find("./m:activation/m:property", NS)
    name = prop.findtext("./m:name", namespaces=NS) if prop is not None else None
    value = prop.findtext("./m:value", namespaces=NS) if prop is not None else None
    if name != ACTIVATION_PROPERTY:
        errors.append(
            f"Profile '{SHARD_PROFILE}' no longer activates on property "
            f"'{ACTIVATION_PROPERTY}', which three shards pass on the command line."
        )
    # Presence-only activation treats any value as active, so -D...=false would skip
    # core's unit tests, and MAVEN_OPTS or .mvn/jvm.config would activate it as readily
    # as the workflow's -D does.
    if value != "true":
        errors.append(
            f"Profile '{SHARD_PROFILE}' must activate on <value>true</value>; without it "
            f"the activation reverts to presence-only and -D{ACTIVATION_PROPERTY}=false "
            "would skip core's unit tests."
        )


def check_unbinds(root, shard, errors):
    """Every execution the profile unbinds must exist in the base <build> of that plugin."""
    for plugin in shard.iterfind("./m:build/m:plugins/m:plugin", NS):
        key = plugin_key(plugin)
        group, artifact = key
        declared = build_execution_ids(root, key)
        if declared is None:
            errors.append(
                f"Profile '{SHARD_PROFILE}' unbinds executions of {group}:{artifact}, "
                "which core/pom.xml's <build> no longer declares."
            )
            continue
        for execution in plugin.iterfind("./m:executions/m:execution", NS):
            execution_id = execution.findtext("./m:id", namespaces=NS)
            if execution.findtext("./m:phase", namespaces=NS) != "none":
                errors.append(
                    f"Profile '{SHARD_PROFILE}' entry {artifact}/{execution_id} is not bound "
                    "to phase none, so it unbinds nothing."
                )
            if execution_id not in declared:
                errors.append(
                    f"Profile '{SHARD_PROFILE}' unbinds {artifact} execution "
                    f"'{execution_id}', which core/pom.xml's <build> no longer declares. The "
                    "entry would merge as a new execution instead of unbinding one, and the "
                    "tests would run in every shard."
                )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Verify core/pom.xml supports the nightly Windows shard arguments"
    )
    parser.add_argument(
        "--pom",
        default="core/pom.xml",
        type=pathlib.Path,
        metavar="FILE",
        help="Path to core/pom.xml. Defaults to core/pom.xml below the current directory.",
    )
    args = parser.parse_args(argv)

    try:
        root = ET.parse(args.pom).getroot()
    except (ET.ParseError, OSError) as error:
        workflow_error(f"Unable to parse {args.pom}: {error}")
        return 1

    errors = []
    for profile_id in DEACTIVATED_PROFILES:
        if find_profile(root, profile_id) is None:
            errors.append(
                f"{args.pom} declares no profile '{profile_id}', which a shard deactivates "
                f"with -P '!{profile_id}'. Maven only warns about that, so the shard would "
                "run suites another shard already covers."
            )

    shard = find_profile(root, SHARD_PROFILE)
    if shard is None:
        errors.append(
            f"{args.pom} declares no profile '{SHARD_PROFILE}', which three shards activate "
            "to drop core's unit test executions."
        )
    else:
        check_activation(shard, errors)
        check_unbinds(root, shard, errors)

    for message in errors:
        workflow_error(message)
    if errors:
        print(f"{len(errors)} shard configuration problem(s) found in {args.pom}.")
        return 1
    print(f"{args.pom} supports the nightly Windows shard arguments.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
