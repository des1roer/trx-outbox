up container
```
make
```

stop

```
make down
```

Kafka UI

http://localhost:8802/ui/clusters/local/brokers

Postgres

steps

```bash
python3 tx.py -u 1 -a 10
# Transaction processed successfully: {'status': 'processed', 'transaction_id': 7270069135592132608, 'balance': '10.00', 'created_at': '2024-12-04 13:39:13.264274'}

python3 tx.py -u 1 -a 10 -i 3849e652-3603-4908-bc07-113853232a18 # retry with same external id
# Transaction processed successfully: {'status': 'processed', 'transaction_id': 7270069135592132608, 'balance': '10.00', 'created_at': '2024-12-04T13:39:13.264274', 'duplicate': True}
```

magnet

```
magnet:?xt=urn:btih:2e3778e1c44bb14e6a1a67cab515ab0523eaa9de&dn=R%26D-%D0%BB%D0%B0%D0%B1%D0%BE%D1%80%D0%B0%D1%82%D0%BE%D1%80%D0%B8%D1%8F%20DevHands%2C%20%D0%92%D0%BB%D0%B0%D0%B4%D0%B8%D0%BC%D0%B8%D1%80%20%D0%9F%D0%B5%D1%80%D0%B5%D0%BF%D0%B5%D0%BB%D0%B8%D1%86%D0%B0%20-%20%20%D0%98%D0%BD%D1%82%D0%B5%D0%BD%D1%81%D0%B8%D0%B2%20%D0%BF%D0%BE%20%D0%BE%D1%87%D0%B5%D1%80%D0%B5%D0%B4%D1%8F%D0%BC%20-%20Kafka%20%D0%B8%20NATS%20%282025%29&xl=3382239169&tr=http%3A%2F%2Fbt2.t-ru.org%2Fann%3Fpk%3D0c282dbc085a98a77ac1cd9c66e38024&tr=http%3A%2F%2Fretracker.local%2Fannounce
```