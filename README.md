# HPE Aruba Central Next Gen collector for Zabbix

Collector Python per monitorare HPE Aruba Networking Central Next Gen tramite API GreenLake/New Central e inviare i dati a Zabbix con `zabbix_sender` e item trapper.

Il collector gira fuori da Zabbix: e' adatto anche a Zabbix Cloud, dove non si possono usare external scripts locali.

## Funzionamento

Il flusso e':

1. Il collector legge uno o piu' workspace da `workspaces.json`.
2. Per ogni workspace genera un access token GreenLake usando `workspace_id`, `client_id` e `client_secret`.
3. Se il workspace e' MSP, legge i tenant e fa token exchange per ogni tenant.
4. Interroga le API New Central per AP e switch.
5. Costruisce le LLD aggregate e i valori raw.
6. Invia tutto all'host trapper Zabbix con `zabbix_sender`.

Il template Zabbix usa item dependent per estrarre status, CPU, memoria, radio, porte e collector health dai master item raw. I raw item hanno history a `0`: servono come sorgente dati, ma non conservano storico JSON.

## 1. Credenziali GreenLake e base URL

Per ogni workspace da monitorare recupera:

- `workspace_id`
- `client_id`
- `client_secret`
- `central_base_url`

In GreenLake entra nel workspace corretto e apri la gestione del workspace. Crea o rigenera una API Client Credential, quindi copia `Client ID` e `Client Secret` e salvali in modo sicuro. Il secret non va pubblicato nel repository.

Per il `workspace_id`, usa l'ID del workspace GreenLake. Nel caso MSP questo e' il workspace MSP; il collector ricavera' poi i tenant tramite API. Nel caso standalone e' il workspace del cliente.

Per `central_base_url`, usa il cluster API di New Central del workspace, ad esempio:

```text
https://de2.api.central.arubanetworks.com
```

Esempi comuni sono `de1`, `de2`, `de3`, `gb1`, `us1`, `us2`, `us4`, `us5`, `us6`, `ca1`, `in1`, `jp1`, `au1`, `ae1`. Se il cluster non e' corretto, l'autenticazione GreenLake puo' funzionare ma le chiamate Central possono tornare errori `401`, `400` o dati vuoti.

Riferimenti ufficiali:

- [HPE GreenLake API Client Credentials](https://developer.greenlake.hpe.com/docs/greenlake/services/credentials/public)
- [HPE GreenLake API authentication](https://developer.greenlake.hpe.com/docs/greenlake/guides/public/authentication/authentication/)
- [HPE GreenLake Workspace Management](https://developer.greenlake.hpe.com/docs/greenlake/services/workspace/public)

## 2. Zabbix

Importa il template:

```text
zabbix_template_hpe_aruba_central_new_ap_trapper.yaml
```

Crea un host in Zabbix con lo stesso nome configurato in `workspaces.json`:

```json
"host": "HPE Aruba Central"
```

Associa il template `HPE Aruba Central New AP by trapper` all'host. Gli item trapper e dependent vengono creati dal template; non serve creare item manuali.

Nel template puoi personalizzare le macro:

```text
{$CENTRAL.COLLECTOR.NODATA}  default 15m
{$CENTRAL.TAG.AP}            default ap
{$CENTRAL.TAG.SWITCH}        default switch
{$CENTRAL.TAG.GATEWAY}       default gateway
```

Gli item scoperti hanno tag:

- `tenant = {#TENANT_NAME}`
- `workspace = {#WORKSPACE_NAME}`
- `device_type = valore della macro per AP/switch/gateway`

La macro gateway e' gia' presente per coerenza, anche se la raccolta gateway non e' ancora implementata.

## 3. Collector

Copia i file del progetto su un server o PC che possa raggiungere:

- GreenLake/New Central via HTTPS
- il server o proxy Zabbix sulla porta trapper, normalmente `10051`

Installa Python 3 e scarica `zabbix_sender` per il sistema operativo del collector. Su Windows puoi lasciare `zabbix_sender.exe` nella stessa cartella e indicarlo nel config.

Copia l'esempio:

```powershell
Copy-Item .\workspaces.example.json .\workspaces.json
```

Compila `workspaces.json`:

```json
{
  "zabbix": {
    "server": "zabbix.example.com",
    "port": "10051",
    "host": "HPE Aruba Central",
    "sender_path": "zabbix_sender"
  },
  "collector": {
    "interval_seconds": 300,
    "collect_client_counts": false
  },
  "workspaces": [
    {
      "name": "WORKSPACE MSP",
      "mode": "msp",
      "workspace_id": "workspace-id",
      "client_id": "client-id",
      "client_secret": "client-secret",
      "central_base_url": "https://de2.api.central.arubanetworks.com",
      "tenant_allowlist": []
    },
    {
      "name": "WORKSPACE STANDALONE",
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

- `msp`: workspace MSP, lista tenant, token exchange per tenant.
- `standalone`: workspace trattato come tenant singolo.

`tenant_allowlist` e' opzionale: se vuoto monitora tutti i tenant MSP; se valorizzato limita la raccolta a tenant ID o tenant name specifici.

`collect_client_counts` e' disabilitato di default per ridurre chiamate API e tempi di raccolta. Abilitalo solo se vuoi raccogliere il numero di client wireless per AP.

`workspaces.json` contiene segreti ed e' ignorato da git.

## 4. Test e schedulazione

Validazione config:

```powershell
python .\central_collector.py config-check
```

Controllo autenticazione:

```powershell
python .\central_collector.py auth-check
```

Riepilogo workspace, tenant e device:

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

Oppure su Windows:

```powershell
.\start-collector-daemon.ps1
```

Schedulazione Windows ogni 5 minuti:

```powershell
schtasks /Create /TN "HPE Central to Zabbix" /SC MINUTE /MO 5 /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -Command cd 'C:\zabbix\hpe-central-zabbix'; python .\central_collector.py push-all" /F
```

Daemon all'avvio sessione Windows:

```powershell
schtasks /Create /TN "HPE Central Collector Daemon" /SC ONLOGON /TR "powershell.exe -NoProfile -ExecutionPolicy Bypass -File 'C:\zabbix\hpe-central-zabbix\start-collector-daemon.ps1'" /F
```

Linux cron:

```cron
*/5 * * * * cd /opt/hpe-central-zabbix && /usr/bin/python3 central_collector.py push-all
```

## Dati inviati

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

## Note di sicurezza

Il collector salva token temporanei in `.token_cache.json`. Il file contiene access token validi circa 15 minuti: proteggi la directory del collector.

Non pubblicare mai:

- `workspaces.json`
- `.token_cache.json`
- `zabbix_sender.exe`

Gli alert Central verranno gestiti solo con New Central API/webhook, non con endpoint Classic Central.
