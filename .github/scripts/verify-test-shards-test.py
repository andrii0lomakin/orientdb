#!/usr/bin/env python3
"""Stand-alone tests for verify-test-shards.py.

Each test mutates a minimal pom the way a real edit to core/pom.xml would, then asserts the
checker's exit status. The mutations are the drift cases the checker exists to catch: a
renamed compliance profile (which makes a shard's -P '!<id>' a silent no-op), a renamed or
rephased unbind target (which makes the shard profile's entry merge as a new execution
instead of unbinding the real one), and an activation that stops matching on the value
(which would let -D...=false skip core's unit tests).
"""

import os
import pathlib
import subprocess
import sys
import tempfile

_DEFAULT_SCRIPT = pathlib.Path(__file__).with_name("verify-test-shards.py")
_SCRIPT = pathlib.Path(os.environ.get("VERIFY_TEST_SHARDS_SCRIPT", _DEFAULT_SCRIPT)).resolve()
_failures = []

# A pom with the same shape as core/pom.xml: two compliance profiles a shard deactivates,
# two base-build plugins whose executions the shard profile unbinds, and the shard profile
# itself. maven-surefire-plugin omits <groupId> on purpose, so the tests also cover the
# checker defaulting it the way Maven does.
POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <artifactId>youtrackdb-core</artifactId>
  <build>
    <plugins>
      <plugin>
        <artifactId>maven-surefire-plugin</artifactId>
        <executions>
          <execution>
            <id>default-test</id>
            <phase>test</phase>
          </execution>
          <execution>
            <id>sequential-tests</id>
            <phase>test</phase>
          </execution>
        </executions>
      </plugin>
      <plugin>
        <groupId>com.vmlens</groupId>
        <artifactId>vmlens-maven-plugin</artifactId>
        <executions>
          <execution>
            <id>test</id>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
  <profiles>
    <profile>
      <id>gremlin-compliance-process-suites</id>
    </profile>
    <profile>
      <id>gremlin-compliance-structure-suites</id>
    </profile>
    <profile>
      <id>shard-skip-core-unit-tests</id>
      <activation>
        <property>
          <name>youtrackdb.shard.skipCoreUnitTests</name>
          <value>true</value>
        </property>
      </activation>
      <build>
        <plugins>
          <plugin>
            <artifactId>maven-surefire-plugin</artifactId>
            <executions>
              <execution>
                <id>default-test</id>
                <phase>none</phase>
              </execution>
              <execution>
                <id>sequential-tests</id>
                <phase>none</phase>
              </execution>
            </executions>
          </plugin>
          <plugin>
            <groupId>com.vmlens</groupId>
            <artifactId>vmlens-maven-plugin</artifactId>
            <executions>
              <execution>
                <id>test</id>
                <phase>none</phase>
              </execution>
            </executions>
          </plugin>
        </plugins>
      </build>
    </profile>
  </profiles>
</project>
"""


def check(name, condition):
    if condition:
        print(f"  PASS  {name}")
    else:
        print(f"  FAIL  {name}")
        _failures.append(name)


def run(pom_text):
    """Run the checker against this pom and return (exit code, combined output)."""
    with tempfile.TemporaryDirectory() as tmp:
        pom = pathlib.Path(tmp) / "pom.xml"
        pom.write_text(pom_text, encoding="utf-8")
        result = subprocess.run(
            [sys.executable, str(_SCRIPT), "--pom", str(pom)],
            capture_output=True,
            text=True,
        )
    return result.returncode, result.stdout + result.stderr


def test_intact_pom_passes():
    """A pom carrying every profile, activation value and unbind target must pass."""
    code, out = run(POM)
    check("intact pom exits 0", code == 0)
    check("intact pom emits no error annotation", "::error::" not in out)


def test_renamed_compliance_profile_fails():
    """Renaming a compliance profile makes a shard's -P '!<id>' a silent no-op."""
    for profile_id in (
        "gremlin-compliance-process-suites",
        "gremlin-compliance-structure-suites",
    ):
        code, out = run(POM.replace(f"<id>{profile_id}</id>", "<id>renamed</id>"))
        check(f"renamed profile {profile_id} exits 1", code == 1)
        check(f"renamed profile {profile_id} is named in the error", profile_id in out)


def test_missing_shard_profile_fails():
    """Without the shard profile, three shards would run core's unit tests again."""
    code, out = run(POM.replace("<id>shard-skip-core-unit-tests</id>", "<id>renamed</id>"))
    check("missing shard profile exits 1", code == 1)
    check("missing shard profile is named in the error", "shard-skip-core-unit-tests" in out)


def test_presence_only_activation_fails():
    """Dropping <value>true</value> would let -D...=false skip core's unit tests."""
    code, out = run(POM.replace("          <value>true</value>\n", ""))
    check("presence-only activation exits 1", code == 1)
    check("presence-only activation error mentions the value", "<value>true</value>" in out)


def test_renamed_activation_property_fails():
    """The property name must match the one the workflow passes."""
    code, out = run(
        POM.replace(
            "<name>youtrackdb.shard.skipCoreUnitTests</name>", "<name>ytdb.shard.skip</name>"
        )
    )
    check("renamed activation property exits 1", code == 1)
    check(
        "renamed activation property is named in the error",
        "youtrackdb.shard.skipCoreUnitTests" in out,
    )


def test_renamed_surefire_execution_fails():
    """A renamed base execution makes the unbind entry merge as a new execution.

    Only the base <build> declaration is renamed, leaving the shard profile pointing at the
    old id, which is what a partial edit looks like in practice.
    """
    broken = POM.replace(
        """          <execution>
            <id>default-test</id>
            <phase>test</phase>
          </execution>""",
        """          <execution>
            <id>renamed-test</id>
            <phase>test</phase>
          </execution>""",
    )
    code, out = run(broken)
    check("renamed surefire execution exits 1", code == 1)
    check("renamed surefire execution is named in the error", "default-test" in out)


def test_removed_vmlens_execution_fails():
    """Regression test for the case that was live: vmlens left bound in every shard.

    vmlens runs **/*MTTest.java in the same `test` phase as surefire, so unbinding surefire
    alone left those classes running in all four shards. The checker must notice if the
    vmlens execution it unbinds stops existing in the base <build>.
    """
    broken = POM.replace(
        """      <plugin>
        <groupId>com.vmlens</groupId>
        <artifactId>vmlens-maven-plugin</artifactId>
        <executions>
          <execution>
            <id>test</id>
          </execution>
        </executions>
      </plugin>
""",
        "",
        1,
    )
    code, out = run(broken)
    check("removed vmlens plugin exits 1", code == 1)
    check("removed vmlens plugin is named in the error", "vmlens-maven-plugin" in out)


def test_unbind_entry_not_phase_none_fails():
    """An unbind entry that does not say phase none unbinds nothing."""
    broken = POM.replace(
        """              <execution>
                <id>sequential-tests</id>
                <phase>none</phase>
              </execution>""",
        """              <execution>
                <id>sequential-tests</id>
                <phase>test</phase>
              </execution>""",
    )
    code, out = run(broken)
    check("unbind entry off phase none exits 1", code == 1)
    check("unbind entry off phase none is named in the error", "sequential-tests" in out)


def test_id_test_is_not_matched_across_plugins():
    """`test` is an execution id in two plugins, so the check must be plugin-scoped.

    A plugin-blind check would see surefire's executions and accept the vmlens unbind, or
    accept vmlens's and miss a surefire rename. Renaming only vmlens's base execution must
    still fail, even though another plugin declares an execution with the same id.
    """
    broken = POM.replace(
        """        <executions>
          <execution>
            <id>test</id>
          </execution>
        </executions>""",
        """        <executions>
          <execution>
            <id>mt-test</id>
          </execution>
        </executions>""",
    )
    code, out = run(broken)
    check("plugin-scoped id matching exits 1", code == 1)
    check("plugin-scoped error names vmlens", "vmlens-maven-plugin" in out)


def test_malformed_pom_fails():
    """A pom that does not parse must fail rather than silently pass."""
    code, out = run("<project><unclosed>")
    check("malformed pom exits 1", code == 1)
    check("malformed pom reports a parse error", "Unable to parse" in out)


def main():
    tests = (
        test_intact_pom_passes,
        test_renamed_compliance_profile_fails,
        test_missing_shard_profile_fails,
        test_presence_only_activation_fails,
        test_renamed_activation_property_fails,
        test_renamed_surefire_execution_fails,
        test_removed_vmlens_execution_fails,
        test_unbind_entry_not_phase_none_fails,
        test_id_test_is_not_matched_across_plugins,
        test_malformed_pom_fails,
    )
    for test in tests:
        print(test.__name__)
        test()
    if _failures:
        print(f"\n{len(_failures)} check(s) failed.")
        return 1
    print(f"\nAll checks passed across {len(tests)} test function(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
