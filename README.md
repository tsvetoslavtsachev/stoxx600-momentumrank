# MomentumRank — STOXX Europe 600

Автоматичен momentum dashboard за всички 600 компании в STOXX Europe 600 индекса.

Live dashboard → `index.html` (отвори директно от GitHub Pages)

---

## Файлова структура

```
├── fetch_data.py          # Python скрипт за изтегляне на данни
├── index.html             # Dashboard (static SPA)
├── data.json              # Генериран автоматично от fetch_data.py
├── data_meta.json         # Метаданни (дата, брой записи)
├── requirements.txt       # Python зависимости
└── .github/
    └── workflows/
        └── update_data.yml  # GitHub Actions workflow
```

---

## Настройка

### 1. Създай GitHub repository

```bash
git init stoxx600-momentumrank
cd stoxx600-momentumrank
# Копирай файловете тук
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/<USERNAME>/stoxx600-momentumrank.git
git push -u origin main
```

### 2. Активирай GitHub Pages

- Settings → Pages → Source: **Deploy from a branch**
- Branch: `main` / `/ (root)`

Dashboard ще е достъпен на:
`https://<USERNAME>.github.io/stoxx600-momentumrank/`

### 3. Провери GitHub Actions

- Actions → **Update STOXX 600 MomentumRank Data** → Run workflow (ръчен тест)
- Workflow-ът се изпълнява **автоматично всеки делничен ден в 08:00 UTC** (11:00 EEST)
- При успех commit-ва `data.json` и `data_meta.json` обратно в repo-то

---

## Как работи

### fetch_data.py

1. **Зарежда списъка** на 600-те компании от Wikipedia (STOXX Europe 600 страница)
2. **Добавя Yahoo Finance суфикси** по страна:
   - Великобритания → `.L` (London)
   - Германия → `.DE` (XETRA)
   - Франция → `.PA` (Euronext Paris)
   - Швейцария → `.SW` (SIX)
   - Холандия → `.AS` (Amsterdam)
   - Швеция → `.ST` (Stockholm)
   - Испания → `.MC` (Madrid)
   - Италия → `.MI` (Milan)
   - Финландия → `.HE` (Helsinki)
   - Белгия → `.BR` (Brussels)
   - Норвегия → `.OL` (Oslo)
   - Дания → `.CO` (Copenhagen)
   - Австрия → `.VI` (Vienna)
   - и др.
3. **Изтегля 400 дни OHLCV история** via `yfinance`
4. **Изчислява метрики:**
   - Return 1M / 3M / 6M / 12M (в local currency, adjusted close)
   - Volatility (annualized std dev на log-returns × √252)
   - Sharpe ratio (rf = 2.5% — Bund yield approximation)
   - Max Drawdown (от цялата история)
   - Market Cap в **EUR** (с FX conversion)
5. **Momentum Score (0–100)** — sigmoid формула:
   - 12M return → 30%
   - 6M return  → 25%
   - 3M return  → 20%
   - 1M return  → 10%
   - Sharpe     → 10%
   - Volatility → 3%
   - Market Cap → 2%

### index.html

Single-page dashboard без зависимости (само SheetJS CDN за Excel export).

**Функции:**
- Сортиране по всяка колона
- Филтри: Sector, **Country** (ново!), Market Cap, Return range, Volatility range, Min Score
- Quick filters: Top 20 / 50 / 100
- Sector bars (clickable → филтрира таблицата)
- Portfolio builder с pie chart
- **↓ Export Table (.xlsx)** — текущо филтрираните редове
- **↓ Export Excel** — селектираното портфолио
- Дата на последен ъпдейт горе вдясно
- Keyboard: `Ctrl+F` = search, `Esc` = clear filters

---

## Ръчно стартиране

```bash
pip install yfinance pandas numpy requests lxml html5lib
python fetch_data.py
```

Отвори `index.html` в браузъра (директно от файловата система).

---

## Разлики спрямо S&P 500 версията

| | S&P 500 | STOXX 600 |
|---|---|---|
| Тикери | директно от GitHub CSV | Wikipedia + Yahoo суфикс по страна |
| Валута | USD | EUR (конвертирана) |
| Risk-free rate | 4.5% (T-Bill) | 2.5% (Bund) |
| Страни | 1 (US) | 17 (UK, DE, FR, CH, NL…) |
| Нов филтър | — | **Country** |
| Нова колона | — | **Country** |
| Тикер формат | `AAPL` | `ASML.AS`, `SAP.DE` |
