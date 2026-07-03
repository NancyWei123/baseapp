import { Channel, eventChannel, EventChannel } from 'redux-saga';
import { all, call, cancel, delay, fork, put, race, select, take, takeEvery } from 'redux-saga/effects';
import { isFinexEnabled, rangerUrl } from '../../../../api';
import { store } from '../../../../store';
import { pushHistoryEmit } from '../../../user/history';
import { selectOpenOrdersList, userOpenOrdersUpdate } from '../../../user/openOrders';
import { userOrdersHistoryRangerData} from '../../../user/ordersHistory';
import { updateWalletsDataByRanger, walletsAddressDataWS } from '../../../user/wallets';
import { alertPush } from '../../alert';
import { klinePush } from '../../kline';
import { Market, marketsTickersData, selectCurrentMarket, selectMarkets, SetCurrentMarket } from '../../markets';
import { MARKETS_SET_CURRENT_MARKET, MARKETS_SET_CURRENT_MARKET_IFUNSET } from '../../markets/constants';
import { depthData, depthDataIncrement, depthDataSnapshot, selectOrderBookSequence } from '../../orderBook';
import { recentTradesPush } from '../../recentTrades';
import {
    RangerConnectFetch,
    rangerDisconnectData,
    rangerDisconnectFetch,
    rangerSubscribeMarket,
    rangerUnsubscribeMarket,
    rangerUserOrderUpdate,
    subscriptionsUpdate,
    UserOrderUpdate,
} from '../actions';
import {
    RANGER_CONNECT_DATA,
    RANGER_CONNECT_FETCH,
    RANGER_DIRECT_WRITE,
    RANGER_DISCONNECT_DATA,
    RANGER_DISCONNECT_FETCH,
    RANGER_USER_ORDER_UPDATE,
} from '../constants';
import { formatTicker, generateSocketURI, streamsBuilder } from '../helpers';
import { selectSubscriptions } from '../selectors';

interface RangerBuffer {
    messages: object[];
}

const USE_BINANCE_PUBLIC_MARKET = true;
const BINANCE_WS_URL = 'wss://stream.binance.com:9443/stream';
const BINANCE_DEFAULT_MARKET = 'btcusdt';
const BINANCE_DEFAULT_KLINE_INTERVAL = '15m';

const BINANCE_MARKET_SYMBOLS: { [market: string]: string } = {
    btcusd: 'btcusdt',
    btcusdt: 'btcusdt',
    ethusd: 'ethusdt',
    ethusdt: 'ethusdt',
    ethbtc: 'ethbtc',
    trxusdt: 'trxusdt',
    bnbusdt: 'bnbusdt',
    xrpusdt: 'xrpusdt',
    ltcusdt: 'ltcusdt',
    bchusdt: 'bchusdt',
    dogeusdt: 'dogeusdt',
    solusdt: 'solusdt',
    adausdt: 'adausdt',
};

const BINANCE_KLINE_INTERVALS: { [period: string]: string } = {
    '1': '1m',
    '1m': '1m',
    '3': '3m',
    '3m': '3m',
    '5': '5m',
    '5m': '5m',
    '15': '15m',
    '15m': '15m',
    '30': '30m',
    '30m': '30m',
    '60': '1h',
    '1h': '1h',
    '120': '2h',
    '2h': '2h',
    '240': '4h',
    '4h': '4h',
    '360': '6h',
    '6h': '6h',
    '480': '8h',
    '8h': '8h',
    '720': '12h',
    '12h': '12h',
    '1440': '1d',
    '1d': '1d',
    '4320': '3d',
    '3d': '3d',
    '10080': '1w',
    '1w': '1w',
};

const normalizeMarketId = (marketId: string | undefined): string => (
    marketId || BINANCE_DEFAULT_MARKET
).toLowerCase().replace(/[-_/]/g, '');

const normalizeBinanceInterval = (period: string | number | undefined): string => {
    const normalizedPeriod = String(period || BINANCE_DEFAULT_KLINE_INTERVAL).toLowerCase();

    return BINANCE_KLINE_INTERVALS[normalizedPeriod] || BINANCE_DEFAULT_KLINE_INTERVAL;
};

const getBinanceSymbolFromMarketId = (marketId: string | undefined): string => {
    const normalizedMarketId = normalizeMarketId(marketId);

    return BINANCE_MARKET_SYMBOLS[normalizedMarketId] || normalizedMarketId;
};

const getBinanceSymbol = (market?: Market): string => getBinanceSymbolFromMarketId(market && market.id);

const getKnownMarkets = (): Market[] => {
    try {
        return selectMarkets(store.getState()) || [];
    } catch (error) {
        return [];
    }
};

const findMarketByBinanceSymbol = (symbol: string, markets: Market[] = getKnownMarkets()): Market | undefined => {
    const normalizedSymbol = symbol.toLowerCase();

    return markets.find(market => getBinanceSymbol(market).toLowerCase() === normalizedSymbol);
};

const getMarketIdByBinanceSymbol = (symbol: string): string => {
    const normalizedSymbol = symbol.toLowerCase();
    const knownMarket = findMarketByBinanceSymbol(normalizedSymbol);

    if (knownMarket) {
        return knownMarket.id;
    }

    if (BINANCE_MARKET_SYMBOLS[normalizedSymbol] === normalizedSymbol) {
        return normalizedSymbol;
    }

    const mappedMarketId = Object.keys(BINANCE_MARKET_SYMBOLS).find(
        marketId => BINANCE_MARKET_SYMBOLS[marketId] === normalizedSymbol,
    );

    return mappedMarketId || normalizedSymbol;
};

const getMarketIdFromBinanceStream = (stream: string): string => {
    const symbol = stream.split('@')[0];

    return getMarketIdByBinanceSymbol(symbol);
};

const rangerStreamToBinanceStream = (stream: string): string | undefined => {
    if (stream === 'global.tickers') {
        return '!ticker@arr';
    }

    const [marketId, channel] = String(stream).split('.');

    if (!marketId || !channel) {
        return;
    }

    const symbol = getBinanceSymbolFromMarketId(marketId);

    if (channel === 'trades') {
        return `${symbol}@trade`;
    }

    if (channel === 'update' || channel === 'ob-inc' || channel === 'ob-snap') {
        return `${symbol}@depth20@100ms`;
    }

    const klineMatch = channel.match(/^kline-(.+)$/);

    if (klineMatch) {
        return `${symbol}@kline_${normalizeBinanceInterval(klineMatch[1])}`;
    }

    return;
};

const binanceSubscriptionMessage = (method: 'SUBSCRIBE' | 'UNSUBSCRIBE', params: string[]) => ({
    method,
    params,
    id: Date.now(),
});

const buildTickerEvent = (event: any, market?: Market) => ({
    amount: String(event.q || '0'),
    name: market ? market.name : String(event.s || '').toUpperCase(),
    base_unit: market ? market.base_unit : '',
    quote_unit: market ? market.quote_unit : '',
    low: String(event.l || '0'),
    high: String(event.h || '0'),
    open: Number(event.o || 0),
    last: String(event.c || '0'),
    avg_price: String(event.w || event.c || '0'),
    price_change_percent: String(event.P || '0'),
    volume: String(event.v || '0'),
    at: Math.floor(Number(event.E || Date.now()) / 1000),
});

const initBinancePublic = (
    market: Market | undefined,
    buffer: RangerBuffer,
): [EventChannel<any>, WebSocket] => {
    const symbol = getBinanceSymbol(market);
    const initialStreams = [
        '!ticker@arr',
        `${symbol}@depth20@100ms`,
        `${symbol}@trade`,
        `${symbol}@kline_${BINANCE_DEFAULT_KLINE_INTERVAL}`,
    ];

    const ws = new WebSocket(`${BINANCE_WS_URL}?streams=${initialStreams.join('/')}`);
    const channel = eventChannel(emitter => {
        ws.onopen = () => {
            emitter({ type: RANGER_CONNECT_DATA });
            while (buffer.messages.length > 0) {
                const message = buffer.messages.shift();
                ws.send(JSON.stringify(message));
            }
        };

        ws.onerror = error => {
            window.console.log('Binance WebSocket error', error);
        };

        ws.onclose = () => {
            emitter(rangerDisconnectData());
            channel.close();
        };

        ws.onmessage = ({ data }) => {
            let message: any;

            try {
                message = JSON.parse(data as string);
            } catch (e) {
                window.console.error('Error parsing Binance data', e);

                return;
            }

            const stream = message.stream;
            const event = message.data;

            if (!stream || !event) {
                return;
            }

            if (stream === '!ticker@arr') {
                const markets = getKnownMarkets();
                const tickerEvents: { [pair: string]: any } = {};

                event.forEach((ticker: any) => {
                    const tickerSymbol = String(ticker.s || '').toLowerCase();
                    const tickerMarket = findMarketByBinanceSymbol(tickerSymbol, markets);

                    if (tickerMarket) {
                        tickerEvents[tickerMarket.id] = buildTickerEvent(ticker, tickerMarket);
                    }
                });

                if (Object.keys(tickerEvents).length > 0) {
                    emitter(marketsTickersData(formatTicker(tickerEvents)));
                }

                return;
            }

            const marketId = getMarketIdFromBinanceStream(stream);
            const streamSymbol = stream.split('@')[0];
            const streamMarket = findMarketByBinanceSymbol(streamSymbol);

            if (stream.indexOf('@depth') !== -1) {
                const bids = event.bids || [];
                const asks = event.asks || [];
                const timestamp = Math.floor(Date.now() / 1000);
                const sequence = Number(event.lastUpdateId) || timestamp;

                emitter(depthData({
                    bids,
                    asks,
                    loading: false,
                    timestamp,
                }));

                emitter(depthDataSnapshot({
                    marketId,
                    bids,
                    asks,
                    sequence,
                    loading: false,
                    timestamp,
                }));

                return;
            }

            if (stream.indexOf('@ticker') !== -1) {
                emitter(marketsTickersData(formatTicker({
                    [marketId]: buildTickerEvent(event, streamMarket),
                })));

                return;
            }

            if (stream.indexOf('@trade') !== -1) {
                emitter(recentTradesPush({
                    market: marketId,
                    trades: [{
                        tid: Number(event.t),
                        taker_type: event.m ? 'sell' : 'buy',
                        date: Math.floor(Number(event.T || Date.now()) / 1000),
                        price: String(event.p),
                        amount: String(event.q),
                    }],
                }));

                return;
            }

            if (stream.indexOf('@kline_') !== -1 && event.k) {
                const k = event.k;
                const period = normalizeBinanceInterval(k.i || stream.split('@kline_')[1]);

                emitter(klinePush({
                    marketId,
                    period,
                    kline: [
                        Math.floor(Number(k.t) / 1000),
                        String(k.o),
                        String(k.h),
                        String(k.l),
                        String(k.c),
                        String(k.v),
                    ],
                }));

                return;
            }
        };

        return () => {
            if (ws.readyState !== ws.CLOSED && ws.readyState !== ws.CLOSING) {
                ws.close();
            }
        };
    });

    return [channel, ws];
};

const initRanger = (
    { withAuth }: RangerConnectFetch['payload'],
    market: Market | undefined,
    prevSubs: string[],
    buffer: RangerBuffer,
): [EventChannel<any>, WebSocket] => {
    const baseUrl = `${rangerUrl()}/${withAuth ? 'private' : 'public'}`;
    const streams = streamsBuilder(withAuth, prevSubs, market);

    const ws = new WebSocket(generateSocketURI(baseUrl, streams));
    const channel = eventChannel(emitter => {
        ws.onopen = () => {
            emitter({ type: RANGER_CONNECT_DATA });
            while (buffer.messages.length > 0) {
                const message = buffer.messages.shift();
                ws.send(JSON.stringify(message));
            }
        };
        ws.onerror = error => {
            window.console.log(`WebSocket error ${error}`);
            window.console.dir(error);
        };
        ws.onclose = event => {
            channel.close();
        };
        ws.onmessage = ({ data }) => {
            let payload: { [pair: string]: any } = {};

            try {
                payload = JSON.parse(data as string);
            } catch (e) {
                window.console.error(`Error parsing : ${e.data}`);
            }

            for (const routingKey in payload) {
                if (payload.hasOwnProperty(routingKey)) {
                    const event = payload[routingKey];

                    const currentMarket = selectCurrentMarket(store.getState());
                    const orderBookMatch = routingKey.match(/([^.]*)\.update/);
                    const orderBookMatchSnap = routingKey.match(/([^.]*)\.ob-snap/);
                    const orderBookMatchInc = routingKey.match(/([^.]*)\.ob-inc/);

                    if (orderBookMatch) {
                        if (currentMarket && orderBookMatch[1] === currentMarket.id) {
                            emitter(depthData(event));
                        }

                        return;
                    }

                    if (orderBookMatchSnap) {
                        if (currentMarket && orderBookMatchSnap[1] === currentMarket.id) {
                            emitter(depthDataSnapshot(event));
                        }

                        return;
                    }

                    if (orderBookMatchInc) {
                        if (currentMarket && orderBookMatchInc[1] === currentMarket.id) {
                            const previousSequence = selectOrderBookSequence(store.getState());
                            if (previousSequence === null) {
                                window.console.log('OrderBook increment received before snapshot');

                                return;
                            }
                            if (previousSequence + 1 !== event.sequence) {
                                window.console.log(`Bad sequence detected in incremental orderbook previous: ${previousSequence}, event: ${event.sequence}`);
                                emitter(rangerDisconnectFetch());

                                return;
                            }
                            emitter(depthDataIncrement(event));
                        }

                        return;
                    }

                    const klineMatch = String(routingKey).match(/([^.]*)\.kline-(.+)/);
                    if (klineMatch) {
                        emitter(
                            klinePush({
                                marketId: klineMatch[1],
                                kline: event,
                                period: klineMatch[2],
                            }),
                        );

                        return;
                    }

                    const tradesMatch = String(routingKey).match(/([^.]*)\.trades/);
                    if (tradesMatch) {
                        emitter(
                            recentTradesPush({
                                trades: event.trades,
                                market: tradesMatch[1],
                            }),
                        );

                        return;
                    }

                    switch (routingKey) {
                        case 'global.tickers':
                            emitter(marketsTickersData(formatTicker(event)));

                            return;

                        case 'success':
                            switch (event.message) {
                                case 'subscribed':
                                case 'unsubscribed':
                                    emitter(subscriptionsUpdate({ subscriptions: event.streams }));

                                    return;
                                default:
                            }

                            return;

                        case 'order':
                            if (isFinexEnabled() && event) {
                                switch (event.state) {
                                    case 'wait':
                                    case 'pending':
                                        const orders = selectOpenOrdersList(store.getState());
                                        const updatedOrder = orders.length && orders.find(order => event.uuid && order.uuid === event.uuid);
                                        if (!updatedOrder) {
                                            emitter(alertPush({ message: ['success.order.created'], type: 'success'}));
                                        }
                                        break;
                                    case 'done':
                                        emitter(alertPush({ message: ['success.order.done'], type: 'success'}));
                                        break;
                                    case 'reject':
                                        emitter(alertPush({ message: ['error.order.rejected'], type: 'error'}));
                                        break;
                                    default:
                                        break;
                                }
                            }

                            emitter(rangerUserOrderUpdate(event));

                            return;

                        case 'trade':
                            emitter(pushHistoryEmit(event));

                            return;

                        case 'balances':
                            emitter(updateWalletsDataByRanger({ ws: true, balances: event }));

                            return;

                        case 'deposit_address':
                            emitter(walletsAddressDataWS(event));

                            return;

                        default:
                    }
                    window.console.log(`Unhandeled websocket channel: ${routingKey}`);
                }
            }
        };

        return () => {
            emitter(rangerDisconnectData());
        };
    });

    return [channel, ws];
};

function* writter(socket: WebSocket, buffer: { messages: object[] }) {
    while (true) {
        const data = yield take(RANGER_DIRECT_WRITE);
        if (socket.readyState === socket.OPEN) {
            socket.send(JSON.stringify(data.payload));
        } else {
            buffer.messages.push(data.payload);
        }
    }
}

function* binanceWritter(socket: WebSocket, buffer: RangerBuffer) {
    while (true) {
        const data = yield take(RANGER_DIRECT_WRITE);
        const payload = data.payload || {};

        if (!payload.streams || !Array.isArray(payload.streams)) {
            continue;
        }

        const params = payload.streams
            .map((stream: string) => rangerStreamToBinanceStream(stream))
            .filter((stream: string | undefined): stream is string => Boolean(stream));

        if (params.length === 0) {
            continue;
        }

        const method = payload.event === 'unsubscribe' ? 'UNSUBSCRIBE' : 'SUBSCRIBE';
        const message = binanceSubscriptionMessage(method, params);

        if (socket.readyState === socket.OPEN) {
            socket.send(JSON.stringify(message));
        } else {
            buffer.messages.push(message);
        }
    }
}

function* reader(channel) {
    while (true) {
        const action = yield take(channel);
        yield put(action);
    }
}

let previousMarket: Market | undefined;

const switchMarket = (subscribeOnInitOnly: boolean) => {
    return function*(action: SetCurrentMarket) {
        if (subscribeOnInitOnly && previousMarket !== undefined) {
            return;
        }
        if (previousMarket && previousMarket.id !== action.payload.id) {
            yield put(rangerUnsubscribeMarket(previousMarket));
        }
        previousMarket = action.payload;
        if (action.payload) {
            yield put(rangerSubscribeMarket(action.payload));
        }
    };
};

function* watchDisconnect(socket: WebSocket, channel: Channel<{}>) {
    yield take(RANGER_DISCONNECT_FETCH);
    socket.close();
}

function* bindSocket(channel: Channel<{}>, socket: WebSocket, buffer: RangerBuffer) {
    return yield all([call(reader, channel), call(writter, socket, buffer), call(watchDisconnect, socket, channel)]);
}

function* bindBinancePublicSocket(channel: Channel<{}>, socket: WebSocket, buffer: RangerBuffer) {
    return yield all([
        call(reader, channel),
        call(binanceWritter, socket, buffer),
        call(watchDisconnect, socket, channel),
    ]);
}

function* dispatchCurrentMarketOrderUpdates(action: UserOrderUpdate) {
    let market;

    try {
        market = yield select(selectCurrentMarket);
    } catch (error) {
        market = undefined;
    }

    if (market && action.payload.market === market.id) {
        yield put(userOpenOrdersUpdate(action.payload));
    }
}

function* dispatchOrderHistoryUpdates(action: UserOrderUpdate) {
    yield put(userOrdersHistoryRangerData(action.payload));
}

function* getSubscriptions() {
    try {
        return yield select(selectSubscriptions);
    } catch (error) {
        return [];
    }
}

export function* rangerSagas() {
    let initialized = false;
    let connectFetchPayload: RangerConnectFetch['payload'] | undefined;
    const buffer: RangerBuffer = { messages: [] };
    const binanceBuffer: RangerBuffer = { messages: [] };
    let pipes;
    yield takeEvery(MARKETS_SET_CURRENT_MARKET, switchMarket(false));
    yield takeEvery(MARKETS_SET_CURRENT_MARKET_IFUNSET, switchMarket(true));
    yield takeEvery(RANGER_USER_ORDER_UPDATE, dispatchCurrentMarketOrderUpdates);
    yield takeEvery(RANGER_USER_ORDER_UPDATE, dispatchOrderHistoryUpdates);

    while (true) {
        const { connectFetch, disconnectData } = yield race({
            connectFetch: take(RANGER_CONNECT_FETCH),
            disconnectData: take(RANGER_DISCONNECT_DATA),
        });
        let market: Market | undefined;

        if (connectFetch) {
            if (initialized) {
                yield put(rangerDisconnectFetch());
                yield take(RANGER_DISCONNECT_DATA);
            }
            connectFetchPayload = connectFetch.payload;
        }

        if (disconnectData) {
            yield delay(1000);
        }

        try {
            market = yield select(selectCurrentMarket);
        } catch (error) {
            market = undefined;
        }

        if (connectFetchPayload) {
            const prevSubs = yield getSubscriptions();
            let channel;
            let socket;
            const useBinancePublic = USE_BINANCE_PUBLIC_MARKET && !connectFetchPayload.withAuth;

            if (useBinancePublic) {
                [channel, socket] = yield call(initBinancePublic, market, binanceBuffer);
            } else {
                [channel, socket] = yield call(initRanger, connectFetchPayload, market, prevSubs, buffer);
            }

            initialized = true;

            if (pipes) {
                yield cancel(pipes);
            }

            pipes = useBinancePublic
                ? yield fork(bindBinancePublicSocket, channel, socket, binanceBuffer)
                : yield fork(bindSocket, channel, socket, buffer);
        }
    }
}