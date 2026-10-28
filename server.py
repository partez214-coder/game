"""Citadels room relay: local HTTP + WebSocket, or Render WebSocket service."""
import argparse
import asyncio
import copy
import json
import logging
import os
from pathlib import Path
import secrets
import signal
import socket
import string
import threading
import webbrowser
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

rooms = {}
PORT_HTTP = 8000
PORT_WS = 8001


def public_player(player):
    return {key: player[key] for key in ('id', 'name', 'avatar', 'isHost')}


def public_room(room):
    return {
        'code': room['code'], 'hostId': room['hostId'],
        'settings': room['settings'],
        'players': [public_player(p) for p in room['players']],
    }


async def send(ws, message):
    try:
        await ws.send(json.dumps(message, ensure_ascii=False))
    except ConnectionClosed:
        pass


async def broadcast(room, message, exclude_ws=None):
    await asyncio.gather(*(send(p['ws'], message) for p in list(room['players'])
                           if p['ws'] != exclude_ws))


async def leave_room(room, player):
    if not room or player not in room['players']:
        return
    room['players'].remove(player)
    if not room['players']:
        rooms.pop(room['code'], None)
        return
    # The host browser runs the game. A disconnected player would leave
    # draft/turn promises unresolved, so end the session explicitly.
    if room['status'] == 'playing':
        room['status'] = 'closed'
        rooms.pop(room['code'], None)
        await broadcast(room, {
            'type': 'room_closed',
            'message': 'Игрок отключился. Создайте новую комнату, чтобы начать партию заново.',
        })
        return
    if room['status'] == 'closed':
        return
    if player['isHost']:
        room['players'][0]['isHost'] = True
        room['hostId'] = room['players'][0]['id']
    await broadcast(room, {
        'type': 'player_left', 'playerId': player['id'],
        'playerName': player['name'], 'newHostId': room['hostId'],
        'players': public_room(room)['players'],
    })


def clean_text(value, default, limit):
    if not isinstance(value, str):
        return default
    return value.replace('<', '').replace('>', '').strip()[:limit] or default


def clean_settings(value):
    value = value if isinstance(value, dict) else {}
    count = value.get('playerCount', 4)
    if type(count) is not int or count not in range(2, 7):
        count = 4
    return {
        'playerCount': count,
        'expansion': 'dark_city' if value.get('expansion') == 'dark_city' else 'classic',
        'targetDistricts': 8 if value.get('targetDistricts') == 8 else 7,
        'aiDifficulty': value.get('aiDifficulty') if value.get('aiDifficulty') in
                        ('normal', 'grandmaster') else 'normal',
    }


def state_for_player(state, player_id):
    """Do not distribute opponents' hands to every browser."""
    result = copy.deepcopy(state)
    hands = result.get('handsByNetworkId', {})
    result['handsByNetworkId'] = {player_id: hands.get(player_id, [])}
    return result


async def ws_handler(ws):
    player = room = None
    try:
        async for raw in ws:
            try:
                data = json.loads(raw)
                if not isinstance(data, dict):
                    raise ValueError()
            except (ValueError, TypeError):
                await send(ws, {'type': 'error', 'message': 'Некорректное сообщение.'})
                continue
            kind = data.get('type')
            if kind == 'leave_room':
                await leave_room(room, player)
                room = player = None
                continue
            if kind in ('create_room', 'join_room'):
                if room and room['status'] == 'closed':
                    await leave_room(room, player)
                    room = player = None
                if room:
                    await send(ws, {'type': 'error', 'message': 'Сначала выйдите из текущей комнаты.'})
                    continue
                if kind == 'create_room':
                    if len(rooms) >= 500:
                        await send(ws, {'type': 'error', 'message': 'Сервер занят. Повторите позже.'})
                        continue
                    while True:
                        code = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(4))
                        if code not in rooms:
                            break
                    room = {'code': code, 'players': [], 'status': 'lobby',
                            'settings': clean_settings(data.get('settings'))}
                    rooms[code] = room
                else:
                    code = clean_text(data.get('code'), '', 6).upper()
                    target = rooms.get(code)
                    error = None
                    if not target:
                        error = 'Комната не найдена. Проверьте код и адрес сервера.'
                    elif target['status'] != 'lobby':
                        error = 'Игра уже началась.'
                    elif len(target['players']) >= target['settings']['playerCount']:
                        error = 'Комната заполнена.'
                    if error:
                        await send(ws, {'type': 'error', 'message': error})
                        continue
                    room = target
                player = {
                    'id': secrets.token_hex(16),
                    'name': clean_text(data.get('name'), 'Правитель', 16),
                    'avatar': clean_text(data.get('avatar'), '🧙', 16),
                    'isHost': kind == 'create_room', 'ws': ws,
                }
                room['players'].append(player)
                if player['isHost']:
                    room['hostId'] = player['id']
                await send(ws, {
                    'type': 'room_created' if player['isHost'] else 'room_joined',
                    'code': room['code'], 'player': public_player(player), 'room': public_room(room),
                })
                if not player['isHost']:
                    await broadcast(room, {
                        'type': 'player_joined', 'player': public_player(player),
                        'players': public_room(room)['players'],
                    }, exclude_ws=ws)
                continue

            if not room or not player or room['status'] == 'closed':
                continue
            if kind == 'chat':
                text = clean_text(data.get('text'), '', 500)
                if text:
                    await broadcast(room, {'type': 'chat_message', 'sender': player['name'],
                                           'avatar': player['avatar'], 'text': text})
            elif kind == 'update_settings' and player['isHost'] and room['status'] == 'lobby':
                settings = clean_settings(data.get('settings'))
                settings['playerCount'] = max(settings['playerCount'], len(room['players']))
                room['settings'] = settings
                await broadcast(room, {'type': 'settings_updated', 'settings': settings})
            elif kind == 'start_game' and player['isHost'] and room['status'] == 'lobby':
                if len(room['players']) < 2:
                    await send(ws, {'type': 'error', 'message': 'Дождитесь хотя бы одного друга.'})
                    continue
                room['status'] = 'playing'
                await broadcast(room, {'type': 'game_started', 'settings': room['settings'],
                                       'players': public_room(room)['players']})
            elif kind == 'sync_game_state' and player['isHost'] and room['status'] == 'playing':
                state, event = data.get('gameState'), data.get('event')
                if not isinstance(state, dict) or not isinstance(state.get('handsByNetworkId', {}), dict):
                    continue
                for guest in list(room['players']):
                    if guest['isHost']:
                        continue
                    guest_event = event if isinstance(event, dict) else None
                    if guest_event and guest_event.get('targetNetworkId') not in (None, guest['id']):
                        guest_event = None
                    await send(guest['ws'], {'type': 'state_update',
                        'gameState': state_for_player(state, guest['id']), 'event': guest_event})
            elif kind == 'player_action' and not player['isHost'] and room['status'] == 'playing':
                host = next((p for p in room['players'] if p['isHost']), None)
                if host and isinstance(data.get('action'), str) and isinstance(data.get('payload', {}), dict):
                    await send(host['ws'], {'type': 'remote_action', 'playerId': player['id'],
                               'action': data['action'], 'payload': data.get('payload', {})})
    except ConnectionClosed:
        pass
    except Exception:
        logging.exception('Room connection failed')
    finally:
        await leave_room(room, player)


def health_check(connection, request):
    if request.path in ('/', '/healthz') and request.headers.get('Upgrade', '').lower() != 'websocket':
        return connection.respond(HTTPStatus.OK, 'Citadels multiplayer server OK\n')


async def main_ws(port=PORT_WS):
    async with serve(ws_handler, '0.0.0.0', port, process_request=health_check,
                     ping_interval=20, ping_timeout=30, max_size=1024 * 1024) as server:
        if os.name != 'nt':
            asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, server.close)
        await server.wait_closed()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cloud', action='store_true', help='Only run the public WebSocket service')
    args = parser.parse_args()
    if args.cloud or os.environ.get('PORT'):
        port = int(os.environ.get('PORT', '8001'))
        print(f'Citadels WebSocket service listening on 0.0.0.0:{port}', flush=True)
    else:
        port = PORT_WS
        from functools import partial
        handler = partial(SimpleHTTPRequestHandler, directory=str(Path(__file__).resolve().parent))
        httpd = ThreadingHTTPServer(('0.0.0.0', PORT_HTTP), handler)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect(('8.8.8.8', 80))
                ip = sock.getsockname()[0]
        except OSError:
            ip = '127.0.0.1'
        print(f'Open http://localhost:{PORT_HTTP} | Friends on Wi-Fi: http://{ip}:{PORT_HTTP}', flush=True)
        webbrowser.open(f'http://localhost:{PORT_HTTP}')
    try:
        asyncio.run(main_ws(port))
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
