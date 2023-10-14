# Longan

Out-of-the-box cryptocurrency quantitative strategy research and live trading frameworks.

This project aims to provide a complete framework that is lightweight, mature, and capable of supporting multi-currency simultaneous trading, and supports secondary development on top of it to realize users' quantitative strategies. This project is completely free and open source.

## Usage

```
pip install -r requirements.txt
```

## Usage

### Market dada

Setting up SQL url and subscribed cryptocurrency types in the binance_md/utils/config.yaml file, then

```
cd binance_md && PYTHON_PATH=../ python3 md.py
```

Setting up local websocket in the litchi_md/config.yaml file, then

```
cd litchi_md && PYTHON_PATH=../ python3 server.py
```

Now you can connect to litchi_md_url and receive the data. 
History data will be cached in disk_cache_folder and SQL database.

### Trade

Under development

## Todo


### binance websocket market data

- [x] depth20
- [x] kline
- [x] aggTrade
- [x] bookTicker
- [x] marketPrice
- [x] use config.yaml
- [x] use multiprocessing
- [ ] support for remote server forwarding
- [ ] performance optimization

### local websocket market data

- [ ] support for remote forwarding
- [ ] trade data
- [ ] Rewriting with golang (due to performance issues)

### binance restful trader

- [x] auth
- [x] limit order
- [ ] market order
- [ ] account / balance management
- [ ] support for remote forwarding
- [ ] performance optimization (use websocket sender)

### local websocket trader

- [ ] insert order
- [ ] trade logs
- [ ] order speed limit
- [ ] on_order
- [ ] on_trade
- [ ] ...

## Contributing

This project is under active development, and there may be major changes in functionality and architecture, so please feel free to submit issues and bugs!

## Thanks

The development of this project is supported by the JetBrains Open Source Development License. [Licenses for Open Source Development - JetBrains](https://www.jetbrains.com/community/opensource/#support)