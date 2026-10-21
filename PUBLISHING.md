# Publishing this action to GitHub Marketplace

Notes for the maintainer. GitHub's guide: [Publishing actions in GitHub Marketplace](https://docs.github.com/en/actions/how-tos/create-and-publish-actions/publish-in-github-marketplace).

## Requirements

- The repository must be **public** and contain a single `action.yml` at the root (this one).
- The `name` in `action.yml` (**CSV and JSON to Parquet**) must be unique: it can't match an existing Marketplace action, a GitHub user or organization, or a Marketplace category. If GitHub reports it as taken, change `name:` (for example to “CSV and JSON to Parquet Converter”) and update the Marketplace link in the README of the main app.
- Your account needs **two-factor authentication**, and the owner of the repository must accept the **GitHub Marketplace Developer Agreement** (GitHub asks for it the first time).

## First release

1. The code is in the public repository `alcazarfran77-dev/csv-to-parquet-action` and the **Test** workflow is green.
2. Go to **Releases → Draft a new release** (or open `action.yml`, where a banner offers **Draft a release**).
3. Tick **Publish this Action to the GitHub Marketplace**. Accept the Marketplace Developer Agreement if GitHub asks (the account owner must do it, with two-factor authentication on). Fix anything the checker reports until it says *Everything looks good!*, and choose a primary category (for example **Utilities**) and, optionally, a second one.
4. **Choose a tag** → type `v1.0.0` → *Create new tag: v1.0.0 on publish*, target `main`. Title: `v1.0.0`. Paste the text of `RELEASE_NOTES.md` as the description.
5. Click **Publish release**.

`uses: alcazarfran77-dev/csv-to-parquet-action@v1` works through the **`v1` branch**, which points to the same commit as `v1.0.0`. Keep it as a branch (don't also create a `v1` tag, so the reference is never ambiguous).

## Later releases

1. Update the engine from the main app if it changed: `scripts/update-engine.sh ../csv-to-parquet`, run `pytest`, commit and push.
2. Draft a new release with the next version (`v1.0.1`, `v1.1.0`…; a new major `v2.0.0` only for breaking changes to the inputs or outputs), keeping **Publish this Action to the GitHub Marketplace** ticked.
3. Move the `v1` branch to the new release: `git fetch --tags && git push -f origin v1.1.0^{commit}:refs/heads/v1`.

To take the action off the Marketplace, edit each published release, untick the publish box and click **Update release**.
