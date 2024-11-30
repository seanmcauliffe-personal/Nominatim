# The Nominatim API classes

The API classes are the core object of the search library. Always instantiate
one of these classes first. The API classes are **not threadsafe**. You need
to instantiate a separate instance for each thread.

## NominatimAPI

::: nominatim_api.NominatimAPI
    options:
        members:
            - **init**
            - config
            - close
            - status
            - details
            - lookup
            - reverse
            - search
            - search_address
            - search_category
        heading_level: 6
        group_by_category: False

### NominatimAPIAsync

::: nominatim_api.NominatimAPIAsync
    options:
        members:
            - **init**
            - setup_database
            - close
            - begin
        heading_level: 6
        group_by_category: False
