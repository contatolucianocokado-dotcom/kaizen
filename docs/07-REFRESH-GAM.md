# Refresh GAM

## Objetivo

Recarregar anuncios somente dentro das regras do Google Ad Manager.

## Principios

- Declarar inventario com refresh no GAM.
- Usar intervalo permitido.
- Nunca refrescar anuncio invisivel.
- Nunca forcar refresh para manipular metricas.
- Registrar key-value de refresh quando aplicavel.

## Exemplo de script

```html
<script>
window.kaizenAdRefresh = {
  minSeconds: 40,
  key: "turnos_refresh",
  value: "time_40s_visible_active"
};
</script>
```

O script final deve ser adaptado ao GPT real do site.
