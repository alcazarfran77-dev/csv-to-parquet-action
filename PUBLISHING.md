# Publishing this action to GitHub Marketplace

Notes for the maintainer. GitHub's guide: [Publishing actions in GitHub Marketplace](https://docs.github.com/en/actions/how-tos/create-and-publish-actions/publish-in-github-marketplace).

## Requirements

- The repository must be **public** and contain a single `action.yml` at the root (this one).
- The `name` in `action.yml` (**CSV and JSON to Parquet**) must be unique: it can't match an existing Marketplace action, a GitHub user or organization, or a Marketplace category. If GitHub reports it as taken, change `name:` (for example to “CSV and JSON to Parquet Converter”) and update the Marketplace link in the README of the main app.
- Your account needs **two-factor authentication**, and the owner of the repository must accept the **GitHub Marketplace Developer Agreement** (GitHub asks for it the first time).

## First release

1. Replace `OWNER` in `README.md` with your GitHub user or organization name.
2. Push to a public repository named `csv-to-parquet-action` and check that the **Test** workflow is green.
3. Open `action.yml` on GitHub: a banner offers to publish the action. Click **Draft a release**.
4. Tick **Publish this Action to the GitHub Marketplace**, fix anything the checker reports until it says *Everything looks good!*, and choose a primary category (for example **Utilities**) and, optionally, a second one.
5. Tag: `v1.0.0`, title: `v1.0.0`. Click **Publish release**.
6. Create the moving major tag so `@v1` works:

   ```bash
   git fetch --tags
   git tag -f v1 v1.0.0
   git push -f origin v1
   ```

## Later releases

1. Update the engine from the main app if it changed: `scripts/update-engine.sh ../csv-to-parquet`, run `pytest`, commit.
2. Draft a new release with the next version (`v1.0.1`, `v1.1.0`…; a new major `v2.0.0` only for breaking changes to the inputs or outputs), keeping **Publish this Action to the GitHub Marketplace** ticked.
3. Move the major tag: `git tag -f v1 v1.1.0 && git push -f origin v1`.

To take the action off the Marketplace, edit each published release, untick the publish box and click **Update release**.
