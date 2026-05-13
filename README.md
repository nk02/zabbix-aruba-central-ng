# HPE Aruba Central Next Gen collector for Zabbix Cloud

Collector Python per monitorare HPE Aruba Networking Central Next Gen con API GreenLake e inviare i dati a Zabbix Cloud tramite `zabbix_sender` e item trapper.

## Configurazione

Copia `workspaces.example.json` in `workspaces.json` e compila:

```json
{
  "zabbix": {
    "server": "zabbix.example.com",
    "port": "10051",
    "host": "HPE Aruba Central RETI",
    "sender_path": "zabbix_sender"
  },
  "collector": {
    "interval_seconds": 300,
    "collect_client_counts": true
  },
  "workspaces": [
    {
      "name": "RETI MSP",
      "mode": "msp",
      "workspace_id": "workspace-id",
      "client_id": "client-id",
      "client_secret": "client-secret",
      "central_base_url": "https://de2.api.central.arubanetworks.com"
    },
    {
      "name": "Cliente diretto",
      "mode": "standalone",
      "workspace_id": "workspace-id",
      "client_id": "client-id",
      "client_secret": "client-secret",
      "central_base_url": "https://de2.api.central.arubanetworks.com"
    }
  ]
}
```

Modalita':

- `msp`: token MSP, lista tenant, token exchange per tenant.
- `standalone`: workspace trattato come tenant singolo.

`workspaces.json` contiene segreti ed e' ignorato da git.

## Zabbix

Importa il template:

```text
zabbix_template_hpe_aruba_central_new_ap_trapper.yaml
```

Crea un host trapper in Zabbix con lo stesso nome configurato in:

```json
"host": "HPE Aruba Central RETI"
```

Associa il template `HPE Aruba Central New AP by trapper` all'host.

## Comandi

Validazione config:

```powershell
python .\central_collector.py config-check
```

Riepilogo workspace/tenant/device:

```powershell
python .\central_collector.py summary
```

Test senza invio:

```powershell
python .\central_collector.py push-all --dry-run
```

Invio a Zabbix:

```powershell
python .\central_collector.py push-all
```

Avvio persistente:

```powershell
python .\central_collector.py daemon --push-command push-all
```

Oppure:

```powershell
.\start-collector-daemon.ps1
```

## Avvio Automatico

Esecuzione periodica ogni 5 minuti:

```powershell
schtasks /Create /TN "HPE Central to Zabbix" /SC MINUTE /MO 5 /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -Command cd 'C:\zabbix\hpe-central-zabbix'; python .\central_collector.py push-all" /F
```

Daemon all'avvio sessione:

```powershell
schtasks /Create /TN "HPE Central Collector Daemon" /SC ONLOGON /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\zabbix\hpe-central-zabbix\start-collector-daemon.ps1'" /F
```

Linux cron:

```cron
*/5 * * * * cd /opt/hpe-central-zabbix && /usr/bin/python3 central_collector.py push-all
```

## Dati Inviati

Collector health:

```text
central.collector.health
```

AP:

```text
central.aps.discovery
central.ap.raw[<tenant-id>,<serial>]
```

Radio AP:

```text
central.ap.radios.discovery
central.ap.radio.raw[<tenant-id>,<serial>,<radio-number>]
```

Switch:

```text
central.switches.discovery
central.switch.raw[<tenant-id>,<serial>]
central.switch.interfaces.discovery
central.switch.interface.raw[<tenant-id>,<serial>,<port-index>]
```

Il collector health include:

- `workspace_count`
- `tenants_count`
- `devices_total`
- `device_counts_by_type`
- `device_counts_by_workspace`
- `sent_lines`
- `elapsed_seconds`

## Note

Il collector salva token temporanei in `.token_cache.json`. Il file contiene access token validi circa 15 minuti: proteggi la directory del collector.

Gli alert Central verranno gestiti solo con New Central API/webhook, non con endpoint Classic Central.
