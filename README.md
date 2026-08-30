# WindUpRelay

Private relay service for the Wind-Up Key Dalamud plugin.

## Expected checkout layout

This repository is designed to be cloned as `WindUpRelay` inside a checkout of
the public `Wind-Up Key Plugin` repository. Its project reference expects the
public protocol project at:

`..\WindUpKey.Protocol\WindUpKey.Protocol.csproj`

## Local configuration

`appsettings.Production.json` is intentionally ignored because it contains the
relay token. Create and maintain that file locally; never commit it.

Build explicitly with:

`dotnet build WindUpRelay.csproj -c Release`

## Linux

`WindUpRelay.Host` is Windows-only. Use the Linux host GUI from the plugin repo root:

```bash
./Start-WindUpHost.sh
```

That opens a Start/Stop window, starts the Production relay on `:8787`, and enables
Tailscale Funnel (same flow as the Windows tray host).

Terminal-only alternative:

```bash
./Start-WindUpRelay.sh           # Production relay on :8787
./Start-WindUpRelay.sh --funnel  # also enable Tailscale Funnel
```

Requires the .NET SDK, `Relay:Token` in `appsettings.Production.json`, and
(for Funnel) the Tailscale CLI with Funnel allowed.
