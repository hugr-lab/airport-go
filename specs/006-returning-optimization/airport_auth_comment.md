# Comment for Airport Auth Issue

---

Great feature request! I'd like to propose an additional mechanism for embedded DuckDB scenarios.

## Use Case: Embedded DuckDB in Applications

When DuckDB is embedded in another application (web server, desktop app, etc.), the host application typically manages user authentication independently. In this case, the auth token is already available in the application context, and we just need a way to pass it to Airport.

## Proposed Solution: Session Variable for Auth Token

Allow secrets to reference a session variable for the auth token:

```sql
-- Create secret that references a session variable
CREATE SECRET airport_api (
    TYPE airport,
    SCOPE 'grpc+tls://api.example.com/',
    AUTH_TOKEN_VARIABLE 'airport_auth_token'
);

-- Application sets the token via session variable (can be updated per-request)
SET airport_auth_token = 'eyJhbGciOiJIUzI1NiIs...';

-- Subsequent queries use the token from the variable
SELECT * FROM airport_api.schema.table;
```

## Benefits

1. **Dynamic token updates** - Token can be refreshed without recreating the secret
2. **Per-session isolation** - Different DuckDB connections can have different tokens
3. **Application control** - Host application manages auth lifecycle (login, refresh, logout)
4. **No secret recreation** - Token rotation doesn't require `DROP SECRET` / `CREATE SECRET`

## Example: Web Application

```python
# Python web server example
@app.route('/api/query')
def handle_query():
    user_token = get_current_user_token()  # From session/JWT

    conn = duckdb.connect()
    conn.execute(f"SET airport_auth_token = '{user_token}'")

    # Secret was created once at startup with AUTH_TOKEN_VARIABLE
    result = conn.execute("SELECT * FROM remote.schema.table").fetchall()
    return jsonify(result)
```

## Interaction with OAuth Flow

This could work alongside the OAuth flow you described:

- **Interactive users**: Use the OAuth redirect flow to obtain token, then set it via variable
- **Service accounts**: Set token directly from environment/config
- **Embedded apps**: Host app manages auth, passes token via variable

Would this be feasible to implement? Happy to discuss further or help with implementation if useful.
