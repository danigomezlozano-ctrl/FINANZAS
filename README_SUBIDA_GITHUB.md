# Paquete definitivo TJL — subida a GitHub

## Archivos que deben quedar en la raíz del repositorio

- `analysis_engine.py`
- `tjl_validation.py`
- `paper_trades.json`
- `test_tjl_validation.py`
- `test_analysis_engine_integration.py`

El workflow `tjl-tests.yml` debe colocarse en:

```text
.github/workflows/tjl-tests.yml
```

## Orden recomendado

1. Crear una rama de seguridad o hacer una copia del estado actual.
2. Subir todos los archivos en el mismo commit.
3. Comprobar que el workflow **TJL validation tests** termina en verde.
4. Ejecutar manualmente una vez el workflow normal del motor.
5. Verificar en `results.json`:
   - `tjl_entry_gate.status = closed`
   - `tjl_validation.sample_count = 18`
   - `tjl_validation.closed = 6`
   - `tjl_validation.open = 12`
   - `tjl_verdict = null`
6. Verificar que una señal TJL aparezca como `SIGNAL_BLOCKED_BY_GATE` y nunca como compra accionable.

## Comandos locales

```bash
python -m py_compile analysis_engine.py tjl_validation.py
python -m py_compile test_tjl_validation.py test_analysis_engine_integration.py
pytest -q
TJL_SELF_TEST=1 python analysis_engine.py
```

Resultado validado antes de entregar:

```text
21 passed
SELF-TESTS TJL: PASS
```

## Estado real de la muestra congelada

El libro entregado contiene **18 operaciones TJL**:

- 6 cerradas
- 12 abiertas

La expectancy neta base provisional de las 6 cerradas es aproximadamente **-4,9767 % por operación**. Estos son los datos presentes en el archivo recibido; no se han inventado dos cierres adicionales para forzar el relato anterior de 8 cerradas y 10 abiertas.

## Protección de integridad

- Hash del protocolo: `de855be08c78`
- Hash de especificación TJL: `33542b259852`
- Hash efectivo del motor: `323fae029218`

Una modificación de las reglas protegidas detiene el motor antes de descargar datos o modificar el libro.
