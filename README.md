Naga-http exposes the Naga rule engine as a web service.

See [Naga](https://github.com/threatgrid/naga) for the rule engine itself.

## Usage

Run the server using [Leiningen](http://leiningen.org):

```bash
  lein ring server
```

Then, submit a logic program to the service:

```bash
  curl -s -H "Content-Type: application/json" -d @../naga/pabu/family.lg http://localhost:3000/eval/pabu | jq .
```

## License

Copyright © 2017 Cisco Systems

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
