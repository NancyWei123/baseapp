import { call, put } from 'redux-saga/effects';
import { sendError } from '../../../';
import { API, isFinexEnabled, RequestOptions } from '../../../../api';
import { buildQueryString, getTimestampPeriod } from '../../../../helpers';
import { klineData, klineError, KlineFetch } from '../actions';

const USE_BINANCE_KLINE = true;
const BINANCE_REST_URL = 'https://api.binance.com';
const BINANCE_DEFAULT_MARKET = 'btcusdt';

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

const klineRequestOptions: RequestOptions = {
    apiVersion: isFinexEnabled() ? 'finex' : 'peatio',
};

const normalizeMarketId = (marketId: string | undefined): string => (
    marketId || BINANCE_DEFAULT_MARKET
).toLowerCase().replace(/[-_/]/g, '');

const getBinanceSymbolFromMarketId = (marketId: string | undefined): string => {
    const normalizedMarketId = normalizeMarketId(marketId);

    return BINANCE_MARKET_SYMBOLS[normalizedMarketId] || normalizedMarketId;
};

const getBinanceInterval = (resolution: number | string): string => {
    const normalizedResolution = String(resolution).toLowerCase();

    return BINANCE_KLINE_INTERVALS[normalizedResolution] || '15m';
};

const requestJson = (url: string) => fetch(url).then(response => {
    if (!response.ok) {
        throw new Error(`Binance kline request failed with status ${response.status}`);
    }

    return response.json();
});

const convertBinanceKlines = (data: any[]) => data.map((elem: any[]) => ({
    date: Number(elem[0]),
    open: Number(elem[1]),
    high: Number(elem[2]),
    low: Number(elem[3]),
    close: Number(elem[4]),
    volume: Number(elem[5]),
}));

const convertBackendKlines = (data: any[]) => data.map(elem => {
    const [date, open, high, low, close, volume] = elem.map(e => {
        switch (typeof e) {
            case 'number':
                return e;
            case 'string':
                return Number.parseFloat(e);
            default:
                throw (new Error(`unexpected type ${typeof e}`));
        }
    });

    return {
        date: date * 1e3,
        open,
        high,
        low,
        close,
        volume,
    };
});

function* fetchBinanceKlineData(market: string, resolution: number, from: string, to: string) {
    const symbol = getBinanceSymbolFromMarketId(market).toUpperCase();
    const interval = getBinanceInterval(resolution);
    const startTime = getTimestampPeriod(from, resolution) * 1000;
    const endTime = getTimestampPeriod(to, resolution) * 1000;
    const query = buildQueryString({
        symbol,
        interval,
        startTime,
        endTime,
        limit: 1000,
    });
    const data = yield call(requestJson, `${BINANCE_REST_URL}/api/v3/klines?${query}`);

    return convertBinanceKlines(data);
}

function* fetchBackendKlineData(market: string, resolution: number, from: string, to: string) {
    const payload = {
        period: resolution,
        time_from: getTimestampPeriod(from, resolution),
        time_to: getTimestampPeriod(to, resolution),
    };

    let endPoint = `/public/markets/${market}/k-line`;

    if (payload) {
        endPoint = `${endPoint}?${buildQueryString(payload)}`;
    }

    const data = yield call(API.get(klineRequestOptions), endPoint);

    return convertBackendKlines(data);
}

export function* handleKlineFetchSaga(action: KlineFetch) {
    const {
        market,
        resolution,
        from,
        to,
    } = action.payload;

    try {
        const convertedData = USE_BINANCE_KLINE
            ? yield call(fetchBinanceKlineData, market, resolution, from, to)
            : yield call(fetchBackendKlineData, market, resolution, from, to);

        yield put(klineData(convertedData));
    } catch (error) {
        try {
            const fallbackData = yield call(fetchBackendKlineData, market, resolution, from, to);
            yield put(klineData(fallbackData));
        } catch (fallbackError) {
            yield put(sendError({
                error: fallbackError,
                processingType: 'alert',
                extraOptions: {
                    actionError: klineError,
                },
            }));
        }
    }
}