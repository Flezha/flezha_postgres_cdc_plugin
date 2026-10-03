import Config

# Compile flezha's SDK the way the controller release does: a controller dials
# no socket, so the SDK's socket client is neither downloaded nor built.
config :flezha_sdk, socket_nif: false
