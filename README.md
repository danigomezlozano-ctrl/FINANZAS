# GeoMacro Intel v5 — Setup en 10 minutos

## Qué hace
- Corre automáticamente cada 6h en GitHub Actions (gratis)
- Calcula RSI, MACD, ATR, Bollinger, VaR, Kelly fraction con matemática pura
- 3 llamadas a Claude: hipótesis → pre-mortem (Kahneman) → calibración con probabilidad
- Dashboard público en GitHub Pages, se actualiza solo

---

## Paso 1 — Subir los archivos al repo FINANZAS

En GitHub, en tu repo, haz clic en **"uploading an existing file"** y sube:
- `analysis_engine.py`
- `index.html`
- `.github/workflows/run.yml` ← esta ruta es importante, crea las carpetas

O desde terminal:
```bash
git clone https://github.com/danigomezlozano-ctrl/FINANZAS.git
cd FINANZAS
# copia los 3 archivos aquí
git add .
git commit -m "init: GeoMacro Intel v5"
git push
```

---

## Paso 2 — Añadir API Keys como Secrets

En tu repo GitHub: **Settings → Secrets and variables → Actions → New repository secret**

| Secret name        | Valor                         | Dónde obtenerlo                          |
|--------------------|-------------------------------|------------------------------------------|
| ANTHROPIC_API_KEY  | sk-ant-api03-...              | console.anthropic.com → API Keys        |
| FRED_API_KEY       | abc123...                     | fred.stlouisfed.org/docs/api/api_key    |
| NEWS_API_KEY       | abc123... (opcional)          | newsapi.org/register                     |

**FRED es gratuito**, regístrate en fred.stlouisfed.org → My Account → API Keys.

---

## Paso 3 — Activar GitHub Pages

En tu repo: **Settings → Pages**
- Source: **Deploy from a branch**
- Branch: **main** / root

En 2 minutos tendrás tu dashboard en:
`https://danigomezlozano-ctrl.github.io/FINANZAS/`

---

## Paso 4 — Primera ejecución manual

Ve a: **Actions → GeoMacro Intel — Análisis Autónomo → Run workflow**

Esto ejecuta el análisis ahora mismo sin esperar al cron.
Verás los logs en tiempo real. Tarda ~3-5 minutos.

Después ya corre solo cada 6h.

---

## Coste estimado

| Componente         | Coste              |
|--------------------|--------------------|
| GitHub Actions     | Gratis (2000 min/mes) |
| GitHub Pages       | Gratis             |
| Yahoo Finance API  | Gratis (no oficial)|
| FRED API           | Gratis             |
| World Bank API     | Gratis             |
| Frankfurter FX     | Gratis             |
| Anthropic (Claude) | ~$0.04 por ejecución × 4/día = ~$5/mes |

---

## Qué calcula el motor (sin IA)

- **RSI 14** — momentum, detección sobreventa/sobrecompra
- **MACD (12,26,9)** — señal de cruce y divergencia
- **SMA 20/50/200** — tendencia corta, media y larga
- **Bollinger Bands (20, 2σ)** — volatilidad y posición relativa del precio
- **ATR 14** — volatilidad real, base para stops
- **Volatilidad anualizada** — desviación estándar de retornos × √252
- **VaR paramétrico 95%** — pérdida máxima esperada 95% del tiempo
- **Max drawdown 6M** — peor caída desde máximo en 6 meses
- **Kelly fraction (×0.25)** — sizing óptimo de posición ajustado conservador
- **Score técnico 0-100** — agregado ponderado de todos los indicadores
- **Score fundamental 0-100** — PIB momentum + ciclo dólar + inventarios
- **Score compuesto** — 40% técnico + 60% fundamental (horizonte medio plazo)

## Framework Kahneman

1. **Hipótesis con chain-of-thought**: Claude razona cada dato antes de concluir
2. **Pre-mortem obligatorio**: "asume que falló — ¿por qué?" elimina overconfidence
3. **Calibración**: probabilidad explícita p=X% ±Y%, condición de invalidación concreta
