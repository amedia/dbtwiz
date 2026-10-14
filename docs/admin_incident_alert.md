# `dbtwiz admin incident-alert`

Post to Slack when a job's latest run escalated or cleared an incident.

Reads the run history Elementary uploads (`dbt_invocations`, `dbt_run_results`) and
groups each model's consecutive failed runs within one orchestrator job into an incident.
Only the given job's latest run is evaluated, and one Slack message is posted when that
run pushed an incident past an escalation threshold — 3h, 6h, 12h, … from its first
failed run, doubling from `incident_alert_escalation_hours` — or cleared one that had
been escalated. Thresholds are in time, not runs, so a daily job escalates at its second
failure.

Escalations are grouped by cause. Each one lists its blast radius from the manifest:
every model downstream of the failing ones, the public/protected ones among them, and
the exposures downstream.

The Slack token is read from `DBTWIZ_SLACK_TOKEN`. Configured in `[tool.dbtwiz.project]`:
`incidents_elementary_dataset` (required), `incidents_window_days`,
`incidents_timezone`, `incident_alert_slack_channel`,
`incident_alert_escalation_hours`, `incident_alert_mentions` and `incident_report_url`
for a link to the report.

`incident_alert_mentions` lists who is notified when something escalates; a recovery
mentions nobody. Each entry is one of:

- **An ID (recommended):** `@U111D1DB111` for a person, `@S012ABCDEF` for a user
  group. Used as is, so it can't fail to resolve, and survives renames. A person's ID is
  under their Slack profile → ⋮ → Copy member ID.
- **A handle:** `@firstname.lastname` for a person (username or display name),
  `@data-team` for a user group. Looked up when posting, which needs the `users:read`
  scope (and `usergroups:read` for groups) and pages through every user, so in a large
  workspace it can hit Slack's rate limit. A handle that isn't found is posted as plain
  text with a note rather than holding the alert back.
- **`@channel` or `@here`:** everyone in the channel, or only those active right now.
  `@here` notifies nobody outside working hours.

## Options

### `--job-id`

Orchestrator job (Airflow DAG id) whose latest run is evaluated.

### `--channel`

Slack channel. Defaults to incident_alert_slack_channel.

### `--manifest-path`

Manifest the blast radius is read from.

### `--as-of`

Evaluate the history as it stood at this UTC time. Only with --dry-run.

### `--dry-run`

Print the message instead of posting it.

## Examples

Show what the hourly job's latest run would post, without posting it:
```shell
$ dbtwiz admin incident-alert --job-id adp__hourly --dry-run
```

Replay a past moment, e.g. to check what an incident looked like while it was ongoing:
```shell
$ dbtwiz admin incident-alert --job-id adp__hourly --as-of 2026-10-14T03:30:00 --dry-run
```
