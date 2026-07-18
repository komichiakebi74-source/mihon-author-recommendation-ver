# Releasing Mihon Recs

Mihon Recs uses the application ID `app.mihon.recs`. Release builds must be signed with a
project-owned key that is backed up permanently. Losing the key prevents future APKs from updating
existing installations.

## Create the signing key once

Keep the key outside the repository. For example:

```powershell
New-Item -ItemType Directory -Force -Path D:\mihon\keys
keytool -genkeypair -v `
  -keystore D:\mihon\keys\mihon-recs-release.jks `
  -alias mihon-recs `
  -keyalg RSA `
  -keysize 4096 `
  -validity 10000
```

Back up the JKS file and its credentials in two secure locations. Never commit them.

## Local signed release

Create the ignored `keystore.properties` file in the repository root:

```properties
storeFile=D:\\mihon\\keys\\mihon-recs-release.jks
storePassword=REPLACE_ME
keyAlias=mihon-recs
keyPassword=REPLACE_ME
```

Then build and verify:

```powershell
.\gradlew.bat spotlessCheck testDebugUnitTest verifySqlDelightMigration assembleRelease
apksigner verify --verbose --print-certs app\build\outputs\apk\release\app-universal-release.apk
```

## GitHub release

Add these repository Actions secrets:

- `SIGNING_KEY`: Base64-encoded contents of the JKS file.
- `KEY_STORE_PASSWORD`: Keystore password.
- `ALIAS`: Key alias.
- `KEY_PASSWORD`: Key password.

PowerShell can create the Base64 value without modifying the JKS file:

```powershell
[Convert]::ToBase64String(
    [IO.File]::ReadAllBytes('D:\mihon\keys\mihon-recs-release.jks')
) | Set-Clipboard
```

Push a version tag to start `.github/workflows/release.yml`:

```powershell
git tag v0.20.1-recs.1
git push origin v0.20.1-recs.1
```

The workflow creates a draft GitHub Release containing the signed universal APK and its SHA-256
checksum. Review the generated notes before publishing it.
