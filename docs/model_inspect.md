# `dbtwiz model inspect`

Output information about a given model.

## Required arguments

- `name`: Model name or path


## Examples

Run the following command to inspect a given model:
```
dbtwiz model inspect mrt_siteconfig__site_groups
```

The command will then output the ancestors and descendants for the model, e.g.
```
Ancestors:
- stg_siteconfig__sites
- int_sitegroups_all_local
- int_sitegroups_all_sites
- int_sitegroups_amedia_local
- int_sitegroups_amedia_owned
- int_sitegroups_classes
- int_sitegroups_mainsites
- int_sitegroups_multisites
- int_sitegroups_regions
- int_sitegroups_sections

Descendants:
- mrt_siteconfig__site_groups_map
- mrt_siteconfig__site_groups_map_with_sections
- mrt_subscriptions__purchases_by_sitegroup
- mrt_subscriptions__upgrades_by_sitegroup
- mrt_subscriptions_bt_tables__abo_sites
```
