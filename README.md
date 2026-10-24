# Sideout game server

Lets two players on different devices and networks play Sideout online with party codes and quick match.

## What it does
- Creates parties with 6-character codes (no 0/O or 1/I/L) and handles joining, "not found", "full" and "already started".
- Relays each player's decisions to the other and keeps them flowing through brief disconnects.
- Gives a player 30 seconds to reconnect during a match (90 seconds if they switched apps) before the other player wins by forfeit. A player who comes back too late is told they lost by forfeit.
- Closes parties after 15 minutes without activity, and runs a quick-match queue.
- Checks every message, rate-limits messages and party creation, filters display names, and never trusts a player for the score.

## Run it on your computer
1. Install Node.js 18 or newer from nodejs.org.
2. In this folder run `npm install`, then `npm start`.
3. Open http://localhost:3000. You should see "Sideout server ok".
4. Run `npm test` to check everything (34 checks).

## Settings (environment variables)
| Name | What it does | Default |
|---|---|---|
| `PORT` | Port to listen on. Hosts like Render set this for you. | 3000 |
| `ALLOWED_ORIGINS` | Address of your game site, for example `https://sideout.netlify.app`. Several can be separated by commas. `*` allows any site. | `*` |
| `PARTY_TTL_MIN` | Minutes without activity before a party closes. | 15 |
| `RECONNECT_SEC` | Seconds a player has to reconnect during a match. Keep it at 30: the game's countdown uses the same number. | 30 |
| `BG_RECONNECT_SEC` | Same, for a player who switched apps. Keep it at 90 to match the game. | 90 |
| `CREATE_PER_MIN` | Parties one address can create per minute. | 10 |
