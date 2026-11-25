# Releasing Pigeon

Releases are cut from `main`. The `Release` workflow does the checking and publishing; you do the
parts that need a decision.

## 1. Prepare the release in a pull request

1. Create a branch and make your changes.
2. Set the new version (for example `0.1.2`) in **all three POMs**: `pom.xml`,
   `pigeon-spring-boot-starter/pom.xml` and `pigeon-core/pom.xml`.
3. Update the dependency snippet in `README.md` to the same version.
4. Add a `## 0.1.2 - YYYY-MM-DD` entry to the top of `CHANGELOG.md`. Its text becomes the GitHub
   release notes. Keep upgrade notes in it if anything needs action.
5. Open the pull request. The `Build and test (Java 21)` and `Build demo image` checks must pass.
6. Merge it.

## 2. Tag the merged commit

```bash
git switch main && git pull
git tag -a v0.1.2 -m "Pigeon 0.1.2"
git push origin v0.1.2
```

That is the only manual step after the merge. Release tags are protected: they cannot be moved or
deleted once pushed, because JitPack caches what it builds from a tag. If a release is wrong, ship the
next patch version instead.

## 3. What the workflow does

When the tag arrives, `.github/workflows/release.yml`:

1. Refuses to continue if the tagged commit is not on `main`.
2. Checks that all three POMs, the README snippet and the changelog agree with the tag.
3. Runs the full build and tests.
4. Creates the GitHub release from the changelog entry (and leaves an existing release alone).
5. Triggers the JitPack build and waits for it. It then downloads the jar and checks that the
   auto-configuration and every `db/pigeon/*.sql` migration are inside it.

If a step fails, the release is left as it was and the log says why. You can see the JitPack build log at
`https://jitpack.io/#olafabregas/pigeon`.

## Branch and tag rules

The rules on `main` and on `v*` tags are described in `.github/rulesets/`. They are kept here so they can
be reviewed and re-applied; GitHub does not apply them automatically. To apply or update one:

```bash
gh api -X POST repos/olafabregas/pigeon/rulesets --input .github/rulesets/protect-main.json
gh api -X POST repos/olafabregas/pigeon/rulesets --input .github/rulesets/protect-release-tags.json
```

To change an existing ruleset, find its id with `gh api repos/olafabregas/pigeon/rulesets` and use
`-X PUT repos/olafabregas/pigeon/rulesets/<id>`.
