#Median aggregate


A typical median query is:

```sql
SELECT median(temp) FROM conditions;
```

## Compiling and installing

To compile and install the extension:

```bash
> make
> make install
```

Note, that depending on installation location, installing the
extension might require super-user permissions.

## Testing

Tests can be run with

```bash
> make installcheck
```


