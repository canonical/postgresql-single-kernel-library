(contribute)=
# Contribute

## Development

## Documentation

### VM & K8s tabs

````{tab-set}
```{tab-item} VM
:sync: vm

content
```
```{tab-item} K8s
:sync: k8s

content
```
````

### Admonitions

````
```{dropdown} <card title>          --> card heading
:open:                              --> card's default state should be expanded 
:class-container: dropdown-<type>   --> "note", "tip", "caution", or "important" 
:icon: <name>                       --> "info", "light-bulb", "alert-fill", or "no-entry-fill"
:class-title: sd-font-weight-normal --> (optional) change title font weight
```
````

```{dropdown} Useful information
:open:
:class-container: dropdown-note
:icon: info

More context about the information
```

```{dropdown} Tip
:open:
:class-container: dropdown-tip
:icon: light-bulb

More context about the tip
```

```{dropdown} Cautionary message
:open:
:class-container: dropdown-caution
:icon: alert-fill

More information about the cautionary message
```

```{dropdown} Important message
:open:
:class-container: dropdown-important
:icon: no-entry-fill

More information about the important message
```