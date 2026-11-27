# Move a pre-0.4 Docker installation to Machines

Use a new database, a new runtime home root and a unique Docker container prefix and namespace. The new runtime API trusts the WebUI server key and verified identity headers; do not expose that key to browsers. Keep the old stack and data intact. A populated 0.2.x database cannot be upgraded in place. See the 0.4 migration in [CHANGELOG](../CHANGELOG.md) for chat, model and other scopes.

1. During an approved maintenance window, freeze WebUI, automation and runtime writes. Make consistent backups of both databases, connection settings and homes. If SQLite is in use, use its backup API or `.backup`; copying a live database file without its WAL is insufficient. Inventory any required files outside mounted homes separately. Perform all subsequent reads against offline copies.
2. Export the selected legacy policy from the copied database. Preserve the exact policy ID as the template ID where possible. Review its image, environment, network, limits and hard caps; pin the candidate runtime image explicitly during rehearsal. The API expects `{"id":"<old-policy-id>","data":{"scope":"user",...}}` at `POST /api/v1/machines/templates`. Remove old allocation flags; do not silently convert shared, per-chat or automation policies to personal workspaces. Leave reset/retention schedules off. Legacy `TERMINALS_OPEN_WEBUI_URL` does not enable JWT verification on this version.
3. In the isolated WebUI database, create or migrate matching server and template access grants and retain **existing user and chat IDs**. Creating a manager template alone does not grant WebUI access or rewrite saved choices. Export the exact WebUI user IDs entitled to that template as a private JSON list. The planner supports default personal homes for one policy only:

   ```sh
   python3 scripts/plan_legacy_user_homes.py --db-snapshot "$OLD_DB_COPY" \
       --homes-snapshot "$OLD_HOMES_COPY" --verified-users "$AUTHORIZED_USERS_JSON" \
       --policy "$OLD_POLICY_ID" --out "$PRIVATE_NEW_PLAN_JSON"
   ```

   It reads directory names and database metadata only, never opens user files, and never copies anything. It only maps a physical home when the verified user has exactly the selected policy and default context in the old state table. Admin/system, orphaned, missing, multi-policy and contextual homes require explicit ownership and access review. Never infer a user ID from a folder name alone. Old default homes could be shared by multiple policies; the new template ID participates in the storage key. Old chat/automation folders use a truncated hash of the full old context under each user's `contexts/`; hashes do not recover IDs. Multiple users of an old chat can map to one new chat workspace; review sharing and collisions before moving anything. Model scope and None are shared across principals and are unsuitable defaults for personal files.
4. For each reviewed mapping, create the target workspace by sending a normal authenticated request with the exact template and verified user to the isolated manager. Confirm the registered instance ID equals the destination basename in the plan. Stop that instance using the manager's stop operation and block reconnections, then stop the candidate manager while copying. Do not reset/delete. Confirm its volume bind source is under the *new* root. Copy exclusively from the offline home snapshot into the matching isolated destination. On Linux, as a user with permission to preserve numeric ownership, inspect the dry run first:

   ```sh
   rsync -aHAXx --numeric-ids --dry-run -- "$OLD_HOMES_COPY/$SOURCE/" "$NEW_HOMES/$DESTINATION/"
   rsync -aHAXx --numeric-ids -- "$OLD_HOMES_COPY/$SOURCE/" "$NEW_HOMES/$DESTINATION/"
   rsync -aHAXx --numeric-ids --checksum --dry-run --itemize-changes -- "$OLD_HOMES_COPY/$SOURCE/" "$NEW_HOMES/$DESTINATION/"
   ```

   Use only the approved plan's source/destination pairs and reviewed, independent roots. No hard links to the original tree, symlink dereferencing, or `--delete`. A checksum pass verifies source files but does not remove or identify files present only in the destination; approve any bootstrap files separately. These commands do not rewrite WebUI permissions or saved selections.
5. With access restricted, test an existing saved selection, first command, file round trip, second user's separate home and cross-chat session access. Release only when every required home and saved selection is accounted for. Restore schedules after verification. For rollback before opening access, stop only the new stack and restore the paired old WebUI/Machines settings, old WebUI/Terminals images and original databases/home roots. After allowing new writes, freeze both sides first and explicitly reconcile new files and state; restoring an old snapshot by itself loses post-cutover writes.
