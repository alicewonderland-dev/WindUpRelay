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
