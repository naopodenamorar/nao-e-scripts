import discord
from discord.ext import commands, tasks
from discord import app_commands
from datetime import datetime, timedelta
import aiosqlite
import re
import random
import asyncio
import time
import os
import logging
import json
from aiohttp import web
from typing import Optional

# ================= CONFIGURAÇÃO =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
TOKEN = os.getenv("DISCORD_TOKEN")

if not TOKEN:
    logger.error("❌ Token não encontrado!")
    exit()
    
ID_DO_SERVIDOR = int(os.getenv("SERVER_ID", "1541930283174076458"))
OWNER_ID = int(os.getenv("OWNER_ID", "1531126567256719622"))
MY_GUILD = discord.Object(id=ID_DO_SERVIDOR)

# ================= DATABASE ASSÍNCRONO =================
class AsyncDatabase:
    def __init__(self, db_name="bot_data.db"):
        self.db_name = db_name
        self.conn = None

    async def init(self):
        self.conn = await aiosqlite.connect(self.db_name)
        self.conn.row_factory = aiosqlite.Row
        await self.create_tables()
        logger.info("✅ Database async OK")

    async def create_tables(self):
        await self.conn.executescript('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                xp INTEGER DEFAULT 0,
                level INTEGER DEFAULT 1,
                coins INTEGER DEFAULT 0,
                reputation INTEGER DEFAULT 0,
                last_daily REAL DEFAULT 0,
                afiliados INTEGER DEFAULT 0
            );
            
            CREATE TABLE IF NOT EXISTS warns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                guild_id INTEGER,
                staff_name TEXT,
                reason TEXT,
                timestamp REAL
            );
            
            CREATE TABLE IF NOT EXISTS invites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                inviter_id INTEGER,
                invited_id INTEGER,
                guild_id INTEGER,
                timestamp REAL
            );
            
            CREATE TABLE IF NOT EXISTS badges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                badge_name TEXT,
                earned_at REAL
            );
            
            CREATE TABLE IF NOT EXISTS guild_config (
                guild_id INTEGER PRIMARY KEY,
                logs_channel INTEGER,
                welcome_channel INTEGER,
                goodbye_channel INTEGER,
                commands_channel INTEGER
            );
            
            CREATE TABLE IF NOT EXISTS raids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER,
                timestamp REAL,
                tipo TEXT,
                detalhes TEXT
            );
            
            CREATE TABLE IF NOT EXISTS sugestoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                guild_id INTEGER,
                sugestao TEXT,
                status TEXT DEFAULT 'pendente',
                timestamp REAL
            );
            
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER,
                nome TEXT,
                conteudo TEXT,
                criador_id INTEGER,
                timestamp REAL
            );
            
            CREATE TABLE IF NOT EXISTS lembretes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                mensagem TEXT,
                tempo_restante REAL,
                criado_em REAL
            );
        ''')
        await self.conn.commit()

    async def get_user(self, user_id):
        async with self.conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
        return None

    async def create_user(self, user_id):
        await self.conn.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        await self.conn.commit()

    async def update_xp(self, user_id, xp_amount):
        await self.create_user(user_id)
        await self.conn.execute("UPDATE users SET xp = xp + ? WHERE user_id = ?", (xp_amount, user_id))
        await self.conn.commit()

    async def level_up(self, user_id):
        await self.conn.execute("UPDATE users SET level = level + 1, xp = 0 WHERE user_id = ?", (user_id,))
        await self.conn.commit()

    async def add_coins(self, user_id, amount):
        await self.create_user(user_id)
        await self.conn.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (amount, user_id))
        await self.conn.commit()

    async def add_reputation(self, user_id, amount):
        await self.create_user(user_id)
        await self.conn.execute("UPDATE users SET reputation = reputation + ? WHERE user_id = ?", (amount, user_id))
        await self.conn.commit()

    async def add_warn(self, user_id, guild_id, staff_name, reason):
        await self.conn.execute(
            "INSERT INTO warns (user_id, guild_id, staff_name, reason, timestamp) VALUES (?, ?, ?, ?, ?)",
            (user_id, guild_id, staff_name, reason, time.time())
        )
        await self.conn.commit()

    async def get_warns(self, user_id, guild_id):
        async with self.conn.execute("SELECT * FROM warns WHERE user_id = ? AND guild_id = ?", (user_id, guild_id)) as cursor:
            return await cursor.fetchall()

    async def get_recent_warns(self, user_id, guild_id, days=7):
        limit_time = time.time() - (days * 86400)
        async with self.conn.execute("SELECT * FROM warns WHERE user_id = ? AND guild_id = ? AND timestamp > ?", (user_id, guild_id, limit_time)) as cursor:
            rows = await cursor.fetchall()
            return len(rows)

    async def clear_warns(self, user_id, guild_id):
        await self.conn.execute("DELETE FROM warns WHERE user_id = ? AND guild_id = ?", (user_id, guild_id))
        await self.conn.commit()

    async def add_sugestao(self, user_id, guild_id, sugestao):
        await self.conn.execute(
            "INSERT INTO sugestoes (user_id, guild_id, sugestao, timestamp) VALUES (?, ?, ?, ?)",
            (user_id, guild_id, sugestao, time.time())
        )
        await self.conn.commit()

    async def get_sugestoes(self, guild_id):
        async with self.conn.execute("SELECT * FROM sugestoes WHERE guild_id = ? ORDER BY timestamp DESC", (guild_id,)) as cursor:
            return await cursor.fetchall()

    async def add_tag(self, guild_id, nome, conteudo, criador_id):
        await self.conn.execute(
            "INSERT INTO tags (guild_id, nome, conteudo, criador_id, timestamp) VALUES (?, ?, ?, ?, ?)",
            (guild_id, nome, conteudo, criador_id, time.time())
        )
        await self.conn.commit()

    async def get_tag(self, guild_id, nome):
        async with self.conn.execute("SELECT * FROM tags WHERE guild_id = ? AND nome = ?", (guild_id, nome)) as cursor:
            return await cursor.fetchone()

    async def get_tags(self, guild_id):
        async with self.conn.execute("SELECT * FROM tags WHERE guild_id = ?", (guild_id,)) as cursor:
            return await cursor.fetchall()

    async def add_lembrete(self, user_id, mensagem, tempo_segundos):
        await self.conn.execute(
            "INSERT INTO lembretes (user_id, mensagem, tempo_restante, criado_em) VALUES (?, ?, ?, ?)",
            (user_id, mensagem, tempo_segundos, time.time())
        )
        await self.conn.commit()

    async def get_lembretes(self, user_id):
        async with self.conn.execute("SELECT * FROM lembretes WHERE user_id = ?", (user_id,)) as cursor:
            return await cursor.fetchall()

    async def get_last_daily(self, user_id):
        async with self.conn.execute("SELECT last_daily FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] is not None else 0

    async def update_last_daily(self, user_id, timestamp):
        await self.create_user(user_id)
        await self.conn.execute("UPDATE users SET last_daily = ? WHERE user_id = ?", (timestamp, user_id))
        await self.conn.commit()

    async def add_raid(self, guild_id, tipo, detalhes):
        await self.conn.execute(
            "INSERT INTO raids (guild_id, timestamp, tipo, detalhes) VALUES (?, ?, ?, ?)",
            (guild_id, time.time(), tipo, detalhes)
        )
        await self.conn.commit()

    async def get_raids(self, guild_id, limit=10):
        async with self.conn.execute("SELECT * FROM raids WHERE guild_id = ? ORDER BY timestamp DESC LIMIT ?", (guild_id, limit)) as cursor:
            return await cursor.fetchall()

    async def get_leaderboard(self, limit=10):
        async with self.conn.execute("SELECT user_id, level, xp, coins, reputation FROM users ORDER BY level DESC, xp DESC LIMIT ?", (limit,)) as cursor:
            return await cursor.fetchall()

    async def get_user_count(self):
        async with self.conn.execute("SELECT COUNT(*) FROM users") as cursor:
            row = await cursor.fetchone()
            return row[0] if row else 0

    async def get_guild_config(self, guild_id):
        async with self.conn.execute("SELECT * FROM guild_config WHERE guild_id = ?", (guild_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def update_guild_config(self, guild_id, **kwargs):
        await self.conn.execute("INSERT OR IGNORE INTO guild_config (guild_id) VALUES (?)", (guild_id,))
        for key, value in kwargs.items():
            await self.conn.execute(f"UPDATE guild_config SET {key} = ? WHERE guild_id = ?", (value, guild_id))
        await self.conn.commit()

# ================= ANTI-RAID =================
class AntiRaid:
    def __init__(self):
        self.joins = []
        
    async def check_join_spam(self, member):
        agora = time.time()
        self.joins.append((member.id, agora))
        self.joins = [(uid, t) for uid, t in self.joins if agora - t < 30]
        
        if len(self.joins) >= 10:
            logger.warning("🚨 JOIN SPAM!")
            return True
        return False
    
    async def check_new_account(self, member):
        idade = (datetime.now() - member.created_at).days
        if idade < 1:
            return True
        return False

# ================= DASHBOARD WEB =================
async def start_web_server():
    async def handle_index(request):
        html = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>🤖 Dashboard Bot DRG</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
            color: #333;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { color: white; text-align: center; margin-bottom: 30px; font-size: 2.5em; text-shadow: 2px 2px 4px rgba(0,0,0,0.3); }
        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 20px; margin-bottom: 30px; }
        .card {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            transition: transform 0.3s, box-shadow 0.3s;
            border-left: 5px solid #667eea;
        }
        .card:hover { transform: translateY(-8px); box-shadow: 0 15px 40px rgba(0,0,0,0.4); }
        .card h2 { color: #667eea; margin-bottom: 15px; font-size: 1.1em; }
        .stat { font-size: 2.5em; font-weight: bold; color: #764ba2; margin: 10px 0; }
        .label { color: #777; font-size: 0.95em; }
        .section {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            margin-bottom: 20px;
        }
        .section h2 { color: #667eea; margin-bottom: 20px; border-bottom: 2px solid #667eea; padding-bottom: 10px; }
        .rank-item {
            display: flex;
            justify-content: space-between;
            padding: 12px;
            border-bottom: 1px solid #eee;
            align-items: center;
        }
        .rank-item:last-child { border-bottom: none; }
        .rank-num { font-weight: bold; color: #667eea; min-width: 30px; }
        .rank-name { flex: 1; margin-left: 15px; }
        .rank-level { background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 5px 12px; border-radius: 20px; font-size: 0.9em; }
        .raid-item {
            padding: 12px;
            background: #fff3cd;
            border-left: 4px solid #ffc107;
            margin-bottom: 10px;
            border-radius: 5px;
        }
        .raid-tipo { font-weight: bold; color: #ff6b6b; }
        .raid-time { color: #666; font-size: 0.85em; margin-top: 5px; }
        .loading { text-align: center; padding: 20px; color: #667eea; }
        .footer { text-align: center; color: white; margin-top: 30px; opacity: 0.8; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🤖 Dashboard Bot DRG</h1>
        
        <div class="grid" id="stats">
            <div class="card"><div class="loading">Carregando...</div></div>
        </div>
        
        <div class="grid">
            <div class="section">
                <h2>🏆 Top 10 Jogadores</h2>
                <div id="leaderboard"><div class="loading">Carregando...</div></div>
            </div>
            
            <div class="section">
                <h2>🚨 Últimos Alertas</h2>
                <div id="raids"><div class="loading">Carregando...</div></div>
            </div>
        </div>
    </div>
    
    <div class="footer">Bot DRG © 2024 | Dashboard v1.0</div>
    
    <script>
        async function load() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                document.getElementById('stats').innerHTML = `
                    <div class="card">
                        <h2>👥 Membros</h2>
                        <div class="stat">${data.members || '-'}</div>
                        <div class="label">No servidor</div>
                    </div>
                    <div class="card">
                        <h2>🎮 Usuários</h2>
                        <div class="stat">${data.users || '-'}</div>
                        <div class="label">Registrados</div>
                    </div>
                    <div class="card">
                        <h2>⭐ Nível Máx</h2>
                        <div class="stat">${data.maxlevel || '-'}</div>
                        <div class="label">Máximo</div>
                    </div>
                    <div class="card">
                        <h2>💰 Total Coins</h2>
                        <div class="stat">${(data.totalcoins || 0).toLocaleString()}</div>
                        <div class="label">Circulando</div>
                    </div>
                `;
                
                const res2 = await fetch('/api/top');
                const top = await res2.json();
                let html = '';
                top.forEach((u, i) => {
                    html += `<div class="rank-item"><span class="rank-num">#${i+1}</span><span class="rank-name"><strong>${u.name}</strong></span><span class="rank-level">⭐ Nível ${u.level}</span></div>`;
                });
                document.getElementById('leaderboard').innerHTML = html || '<div class="loading">Sem dados</div>';
                
                const res3 = await fetch('/api/raids');
                const raids = await res3.json();
                let html2 = '';
                if(raids.length === 0) {
                    html2 = '<p style="text-align: center; color: #4CAF50; padding: 20px;">✅ Nenhum alerta!</p>';
                } else {
                    raids.forEach(r => {
                        const d = new Date(r.timestamp * 1000).toLocaleString('pt-BR');
                        html2 += `<div class="raid-item"><div class="raid-tipo">🚨 ${r.tipo}</div><div class="raid-time">${d}</div><div>${r.detalhes}</div></div>`;
                    });
                }
                document.getElementById('raids').innerHTML = html2;
            } catch(e) { console.error(e); }
        }
        
        load();
        setInterval(load, 5000);
    </script>
</body>
</html>"""
        return web.Response(text=html, content_type='text/html; charset=utf-8')

    async def api_stats(request):
        guild = bot_instance.get_guild(ID_DO_SERVIDOR)
        if not guild:
            return web.json_response({})
        
        users = await db.get_user_count()
        top = await db.get_leaderboard(limit=1)
        maxlevel = top[0]['level'] if top else 0
        
        async with db.conn.execute("SELECT SUM(coins) FROM users") as cursor:
            row = await cursor.fetchone()
            totalcoins = row[0] if row[0] else 0
        
        return web.json_response({
            "members": guild.member_count,
            "users": users,
            "maxlevel": maxlevel,
            "totalcoins": totalcoins
        })

    async def api_top(request):
        top = await db.get_leaderboard(limit=10)
        result = []
        for row in top:
            try:
                user = await bot_instance.fetch_user(row['user_id'])
                result.append({"name": user.name, "level": row['level'], "coins": row['coins']})
            except:
                result.append({"name": f"User#{row['user_id']}", "level": row['level'], "coins": row['coins']})
        return web.json_response(result)

    async def api_raids(request):
        raids = await db.get_raids(ID_DO_SERVIDOR, limit=5)
        result = []
        for raid in raids:
            result.append({
                "tipo": raid['tipo'],
                "detalhes": raid['detalhes'],
                "timestamp": raid['timestamp']
            })
        return web.json_response(result)

    app = web.Application()
    app.router.add_get('/', handle_index)
    app.router.add_get('/api/stats', api_stats)
    app.router.add_get('/api/top', api_top)
    app.router.add_get('/api/raids', api_raids)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    logger.info("🌐 Dashboard em http://0.0.0.0:8080")

# ================= MODALS E VIEWS =================
class PainelTicketView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎫 Abrir Ticket", style=discord.ButtonStyle.primary, custom_id="ticket1")
    async def criar(self, i: discord.Interaction, b: discord.ui.Button):
        await i.response.defer(ephemeral=True)
        ow = {
            i.guild.default_role: discord.PermissionOverwrite(read_messages=False),
            i.user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
            i.guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True)
        }
        ch = await i.guild.create_text_channel(f"ticket-{i.user.name}", overwrites=ow)
        embed = discord.Embed(title="🎫 Ticket Aberto", description=f"Olá {i.user.mention}! Sua solicitação foi criada.", color=discord.Color.green())
        await ch.send(embed=embed)
        await i.followup.send(f"✅ {ch.mention}", ephemeral=True)

class SorteioView(discord.ui.View):
    def __init__(self, premio):
        super().__init__(timeout=None)
        self.premio = premio
        self.participantes = set()

    @discord.ui.button(label="🎉 Participar", style=discord.ButtonStyle.success, custom_id="sort1")
    async def part(self, i: discord.Interaction, b: discord.ui.Button):
        if i.user.id in self.participantes:
            return await i.response.send_message("Já está inscrito!", ephemeral=True)
        self.participantes.add(i.user.id)
        await i.response.send_message("✅ Inscrito no sorteio!", ephemeral=True)

class QuizView(discord.ui.View):
    def __init__(self, opcoes, correta):
        super().__init__(timeout=30)
        self.opcoes = opcoes
        self.correta = correta
        self.respondeu = False

    @discord.ui.button(label="A", style=discord.ButtonStyle.primary, custom_id="q1")
    async def a(self, i: discord.Interaction, b: discord.ui.Button):
        await self.resp(i, 0)

    @discord.ui.button(label="B", style=discord.ButtonStyle.primary, custom_id="q2")
    async def b(self, i: discord.Interaction, b: discord.ui.Button):
        await self.resp(i, 1)

    @discord.ui.button(label="C", style=discord.ButtonStyle.primary, custom_id="q3")
    async def c(self, i: discord.Interaction, b: discord.ui.Button):
        await self.resp(i, 2)

    @discord.ui.button(label="D", style=discord.ButtonStyle.primary, custom_id="q4")
    async def d(self, i: discord.Interaction, b: discord.ui.Button):
        await self.resp(i, 3)

    async def resp(self, i: discord.Interaction, escolha):
        if self.respondeu:
            return
        self.respondeu = True
        if escolha == self.correta:
            await i.response.send_message("✅ Correto! +50 XP +100 coins!", ephemeral=True)
            await db.update_xp(i.user.id, 50)
            await db.add_coins(i.user.id, 100)
        else:
            await i.response.send_message(f"❌ Resposta correta: {self.opcoes[self.correta]}", ephemeral=True)

# ================= DADOS ESTÁTICOS =================
PIADAS = [
    "Por que o livro de matemática se suicidou? Porque tinha muitos problemas!",
    "O que o Python disse para o JavaScript? Você é apenas um roteiro!",
    "Como matar a bateria de um programador? Alt+F4!",
    "Um SQL caminha em um bar, vira para dois bancos e diz: 'Posso juntar vocês?'",
    "Por que os programadores preferem dark mode? Porque a luz atrai bugs!",
]

FATOS = [
    "🌍 A Terra gira ao redor do Sol",
    "🦖 Dinossauros foram extintos há 66 milhões de anos",
    "🐙 Polvos têm 3 corações",
    "🧠 Usamos 100% do nosso cérebro, não apenas 10%",
    "🚀 Vênus é o planeta mais quente do Sistema Solar",
]

QUIZZES = [
    {"pergunta": "Capital da França?", "opcoes": ["Paris", "Londres", "Berlim", "Madrid"], "correta": 0},
    {"pergunta": "Maior planeta?", "opcoes": ["Saturno", "Marte", "Júpiter", "Vênus"], "correta": 2},
    {"pergunta": "Quando terminou 2ª Guerra?", "opcoes": ["1943", "1944", "1945", "1946"], "correta": 2},
    {"pergunta": "Capital do Brasil?", "opcoes": ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador"], "correta": 2},
]

# ================= BOT PRINCIPAL =================
class MeuBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.guilds = True
        super().__init__(command_prefix="!", intents=intents, help_command=None)
        self.anti_raid = AntiRaid()

    async def setup_hook(self):
        await db.init()
        self.add_view(PainelTicketView())
        self.tree.copy_global_to(guild=MY_GUILD)
        await self.tree.sync(guild=MY_GUILD)
        logger.info("✅ Bot pronto!")
        
        asyncio.create_task(start_web_server())
        self.lembrete_loop.start()

    async def on_ready(self):
        logger.info(f"🤖 {self.user} online!")

    @tasks.loop(seconds=10)
    async def lembrete_loop(self):
        try:
            async with db.conn.execute("SELECT id, user_id, mensagem FROM lembretes") as cursor:
                lembretes = await cursor.fetchall()
                for lid, uid, msg in lembretes:
                    try:
                        user = await self.fetch_user(uid)
                        embed = discord.Embed(title="🔔 Lembrete!", description=msg, color=discord.Color.blue())
                        await user.send(embed=embed)
                        await db.conn.execute("DELETE FROM lembretes WHERE id = ?", (lid,))
                    except:
                        pass
            await db.conn.commit()
        except:
            pass

    async def on_member_join(self, member):
        logger.info(f"➕ {member.name}")
        
        if await self.anti_raid.check_join_spam(member):
            await member.ban(reason="[ANTI-RAID] Join spam")
            await db.add_raid(member.guild.id, "Ban Automático", f"Join spam: {member.name}")
            return
        
        if await self.anti_raid.check_new_account(member):
            await member.ban(reason="[ANTI-RAID] Conta muito nova")
            return
        
        config = await db.get_guild_config(member.guild.id)
        if config and config['welcome_channel']:
            try:
                ch = member.guild.get_channel(config['welcome_channel'])
                if ch:
                    embed = discord.Embed(title=f"👋 Bem-vindo, {member.name}!", description=f"Olá {member.mention}!", color=discord.Color.green())
                    embed.set_thumbnail(url=member.display_avatar.url)
                    await ch.send(embed=embed)
            except:
                pass
        
        try:
            embed = discord.Embed(
                title="📜 Regras",
                description="Bem-vindo! Leia as regras:\n1️⃣ Respeito\n2️⃣ Sem preconceito\n3️⃣ Sem spam\n4️⃣ Divirta-se!",
                color=discord.Color.blue()
            )
            await member.send(embed=embed)
        except:
            pass

    async def on_member_remove(self, member):
        config = await db.get_guild_config(member.guild.id)
        if config and config['goodbye_channel']:
            try:
                ch = member.guild.get_channel(config['goodbye_channel'])
                if ch:
                    embed = discord.Embed(title="👋", description=f"{member.name} saiu", color=discord.Color.red())
                    await ch.send(embed=embed)
            except:
                pass

    async def on_message(self, message):
        if message.author.bot or message.guild is None:
            return

        # AutoMod
        if "discord.gg/" in message.content or "discord.com/invite/" in message.content:
            await message.delete()
            await message.author.send("❌ Links de Discord não permitidos!")
            await db.add_warn(message.author.id, message.guild.id, "AutoMod", "Link Discord")
            return
        
        if len(message.mentions) > 5:
            await message.delete()
            await message.author.send("❌ Spam de menções!")
            return

        # XP
        await db.update_xp(message.author.id, random.randint(15, 25))
        user = await db.get_user(message.author.id)
        if user and user['xp'] >= user['level'] * 150:
            await db.level_up(message.author.id)
            await message.channel.send(f"🎉 {message.author.mention} nível {user['level'] + 1}!")

        await self.process_commands(message)

bot_instance = None
bot = MeuBot()
db = AsyncDatabase()

async def enviar_log(guild, embed):
    config = await db.get_guild_config(guild.id)
    if config and config['logs_channel']:
        ch = guild.get_channel(config['logs_channel'])
        if ch:
            try:
                await ch.send(embed=embed)
            except:
                pass

# ================= COMANDOS GERAIS =================
@bot.tree.command(name="help", description="📚 Ajuda com comandos")
async def help_cmd(i: discord.Interaction):
    embed = discord.Embed(title="📚 Comandos Disponíveis", color=discord.Color.blurple())
    embed.add_field(name="👤 Geral", value="/ping /rank /leaderboard /perfil /avatar /info-server /user-info /status", inline=False)
    embed.add_field(name="💰 Economia", value="/balance /daily /work /give-money /remove-money /loja /comprar", inline=False)
    embed.add_field(name="🎮 Games", value="/pedra-papel-tesoura /dados /moeda /piada /fato /pergunta /girar-roleta /quiz", inline=False)
    embed.add_field(name="🛡️ Moderação", value="/warn /warns /mute /kick /ban /purge /lockall /unlockall /limpar-warns /historico", inline=False)
    embed.add_field(name="🎉 Diversão", value="/sorteio /enquete /sugerir /sugestoes /tag /lembrar /meus-lembretes /rep", inline=False)
    embed.add_field(name="⚙️ Setup", value="/setup /config-show /resetar-config /raid-logs /raid-status", inline=False)
    await i.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="ping", description="🏓 Ping do bot")
async def ping(i: discord.Interaction):
    await i.response.send_message(f"🏓 Pong! **{round(bot.latency * 1000)}ms**")

@bot.tree.command(name="rank", description="📈 Ver seu nível")
async def rank(i: discord.Interaction, membro: discord.Member = None):
    alvo = membro or i.user
    await db.create_user(alvo.id)
    user = await db.get_user(alvo.id)
    embed = discord.Embed(title=f"📈 {alvo.display_name}", color=discord.Color.gold())
    embed.add_field(name="⭐ Nível", value=user['level'], inline=True)
    embed.add_field(name="✨ XP", value=f"{user['xp']}/{user['level'] * 150}", inline=True)
    embed.add_field(name="💰 Coins", value=user['coins'], inline=True)
    embed.set_thumbnail(url=alvo.display_avatar.url)
    await i.response.send_message(embed=embed)

@bot.tree.command(name="leaderboard", description="🏆 Top 10 jogadores")
async def leaderboard(i: discord.Interaction):
    await i.response.defer()
    top = await db.get_leaderboard(limit=10)
    embed = discord.Embed(title="🏆 Leaderboard", color=discord.Color.gold())
    desc = ""
    for idx, row in enumerate(top, 1):
        try:
            user = await bot.fetch_user(row['user_id'])
            nome = user.name
        except:
            nome = f"User#{row['user_id']}"
        desc += f"{idx}. **{nome}** - Nível {row['level']}\n"
    embed.description = desc or "Sem dados"
    await i.followup.send(embed=embed)

@bot.tree.command(name="perfil", description="👤 Perfil completo")
async def perfil(i: discord.Interaction, membro: discord.Member = None):
    alvo = membro or i.user
    await db.create_user(alvo.id)
    user = await db.get_user(alvo.id)
    embed = discord.Embed(title=f"👤 {alvo.display_name}", color=discord.Color.purple())
    embed.set_thumbnail(url=alvo.display_avatar.url)
    embed.add_field(name="⭐ Nível", value=user['level'])
    embed.add_field(name="💰 Coins", value=user['coins'])
    embed.add_field(name="👑 Reputação", value=user['reputation'])
    embed.add_field(name="📤 Afiliados", value=user['afiliados'])
    await i.response.send_message(embed=embed)

@bot.tree.command(name="avatar", description="🖼️ Ver avatar")
async def avatar(i: discord.Interaction, membro: discord.Member = None):
    alvo = membro or i.user
    embed = discord.Embed(title=f"Avatar de {alvo.name}", color=discord.Color.blue())
    embed.set_image(url=alvo.display_avatar.url)
    await i.response.send_message(embed=embed)

@bot.tree.command(name="info-server", description="ℹ️ Informações do servidor")
async def info_server(i: discord.Interaction):
    g = i.guild
    embed = discord.Embed(title=f"ℹ️ {g.name}", color=discord.Color.blue())
    embed.add_field(name="👥 Membros", value=g.member_count)
    embed.add_field(name="📅 Criado em", value=g.created_at.strftime("%d/%m/%Y"))
    embed.add_field(name="👑 Owner", value=f"<@{g.owner_id}>")
    if g.icon:
        embed.set_thumbnail(url=g.icon.url)
    await i.response.send_message(embed=embed)

@bot.tree.command(name="user-info", description="👤 Informações do usuário")
async def user_info(i: discord.Interaction, membro: discord.Member = None):
    alvo = membro or i.user
    embed = discord.Embed(title=f"👤 {alvo.display_name}", color=discord.Color.green())
    embed.add_field(name="ID", value=alvo.id)
    embed.add_field(name="Criado em", value=alvo.created_at.strftime("%d/%m/%Y"))
    embed.add_field(name="Entrou em", value=alvo.joined_at.strftime("%d/%m/%Y") if alvo.joined_at else "N/A")
    embed.add_field(name="Cargos", value=", ".join([r.name for r in alvo.roles[1:]]) or "Nenhum")
    embed.set_thumbnail(url=alvo.display_avatar.url)
    await i.response.send_message(embed=embed)

@bot.tree.command(name="status", description="🟢 Status do bot")
async def status(i: discord.Interaction):
    embed = discord.Embed(title="🟢 Status", color=discord.Color.green())
    embed.add_field(name="Ping", value=f"{round(bot.latency * 1000)}ms")
    embed.add_field(name="Versão", value=f"Discord.py {discord.__version__}")
    embed.add_field(name="Dashboard", value="http://localhost:8080")
    await i.response.send_message(embed=embed, ephemeral=True)

# ================= COMANDOS ECONOMIA =================
@bot.tree.command(name="balance", description="💰 Ver saldo")
async def balance(i: discord.Interaction):
    await db.create_user(i.user.id)
    user = await db.get_user(i.user.id)
    await i.response.send_message(f"💰 **{user['coins']} coins**")

@bot.tree.command(name="daily", description="💸 Resgate diário (500 coins)")
async def daily(i: discord.Interaction):
    await db.create_user(i.user.id)
    user = await db.get_user(i.user.id)
    agora = time.time()
    
    if agora - user['last_daily'] < 86400:
        restante = int(86400 - (agora - user['last_daily']))
        h = restante // 3600
        m = (restante % 3600) // 60
        return await i.response.send_message(f"⏳ Tente em **{h}h {m}m**", ephemeral=True)
    
    await db.add_coins(i.user.id, 500)
    await db.update_last_daily(i.user.id, agora)
    await i.response.send_message("💸 **500 coins** ✅")

@bot.tree.command(name="work", description="💼 Trabalhar (50-200 coins)")
async def work(i: discord.Interaction):
    ganho = random.randint(50, 200)
    await db.add_coins(i.user.id, ganho)
    await i.response.send_message(f"💼 +**{ganho} coins**!")

@bot.tree.command(name="give-money", description="💵 Dar coins (Admin)")
@app_commands.default_permissions(administrator=True)
async def give_money(i: discord.Interaction, membro: discord.Member, qtd: int):
    await db.add_coins(membro.id, qtd)
    await i.response.send_message(f"💵 +{qtd} coins para {membro.mention}", ephemeral=True)

@bot.tree.command(name="remove-money", description="💸 Remover coins (Admin)")
@app_commands.default_permissions(administrator=True)
async def remove_money(i: discord.Interaction, membro: discord.Member, qtd: int):
    await db.create_user(membro.id)
    user = await db.get_user(membro.id)
    novo = max(0, user['coins'] - qtd)
    await db.add_coins(membro.id, novo - user['coins'])
    await i.response.send_message(f"💸 -{qtd} coins de {membro.mention}", ephemeral=True)

@bot.tree.command(name="rep", description="👍 Dar reputação")
async def rep(i: discord.Interaction, membro: discord.Member):
    if membro.id == i.user.id:
        return await i.response.send_message("❌ Não pode reputar a si mesmo!", ephemeral=True)
    await db.add_reputation(membro.id, 1)
    await i.response.send_message(f"👍 +1 rep para {membro.mention}!")

@bot.tree.command(name="loja", description="🛒 Ver loja")
async def loja(i: discord.Interaction):
    embed = discord.Embed(title="🛒 Loja", color=discord.Color.gold())
    embed.add_field(name="💎 VIP BRONZE", value="2000 coins\nCargo especial + 2x XP", inline=False)
    embed.add_field(name="💎 VIP PRATA", value="5000 coins\nCargo + 3x XP + Emoji", inline=False)
    embed.add_field(name="💎 VIP OURO", value="10000 coins\nCargo + 5x XP + Tag", inline=False)
    await i.response.send_message(embed=embed)

@bot.tree.command(name="comprar", description="💳 Comprar item")
async def comprar(i: discord.Interaction, item: str):
    precos = {"vip-bronze": 2000, "vip-prata": 5000, "vip-ouro": 10000}
    
    if item not in precos:
        return await i.response.send_message("❌ Item não existe", ephemeral=True)
    
    user = await db.get_user(i.user.id)
    if not user or user['coins'] < precos[item]:
        return await i.response.send_message("❌ Coins insuficientes!", ephemeral=True)
    
    await db.add_coins(i.user.id, -precos[item])
    await i.response.send_message(f"✅ Compra realizada! -{precos[item]} coins")

# ================= COMANDOS GAMES =================
@bot.tree.command(name="pedra-papel-tesoura", description="🎮 Joga com alguém")
async def ppt(i: discord.Interaction, membro: discord.Member):
    if membro.id == i.user.id:
        return await i.response.send_message("❌ Joga com outro!", ephemeral=True)
    opcoes = ["🪨 Pedra", "📄 Papel", "✂️ Tesoura"]
    bot_escolha = random.choice(opcoes)
    resultado = "Você ganhou!" if random.random() > 0.5 else "Você perdeu!"
    embed = discord.Embed(title="🎮 Pedra-Papel-Tesoura", description=f"Você jogou contra {membro.name}!", color=discord.Color.blue())
    embed.add_field(name="Bot escolheu", value=bot_escolha)
    embed.add_field(name="Resultado", value=resultado)
    await i.response.send_message(embed=embed)

@bot.tree.command(name="dados", description="🎲 Rola um dado")
async def dados(i: discord.Interaction):
    resultado = random.randint(1, 6)
    embed = discord.Embed(title="🎲 Dados", description=f"Resultado: **{resultado}**", color=discord.Color.green())
    await i.response.send_message(embed=embed)

@bot.tree.command(name="moeda", description="🪙 Cara ou Coroa")
async def moeda(i: discord.Interaction):
    resultado = "🟡 Cara" if random.random() > 0.5 else "⚫ Coroa"
    await i.response.send_message(f"🪙 {resultado}")

@bot.tree.command(name="piada", description="😂 Uma piada")
async def piada(i: discord.Interaction):
    embed = discord.Embed(title="😂 Piada", description=random.choice(PIADAS), color=discord.Color.gold())
    await i.response.send_message(embed=embed)

@bot.tree.command(name="fato", description="📚 Um fato curioso")
async def fato(i: discord.Interaction):
    embed = discord.Embed(title="📚 Fato", description=random.choice(FATOS), color=discord.Color.blue())
    await i.response.send_message(embed=embed)

@bot.tree.command(name="pergunta", description="🔮 Bola de cristal")
async def pergunta(i: discord.Interaction, pergunta: str):
    respostas = ["Sim ✅", "Não ❌", "Talvez 🤔", "Com certeza!", "Duvido!", "Definitivamente!"]
    embed = discord.Embed(title="🔮", description=f"**{pergunta}**\n\nResposta: **{random.choice(respostas)}**", color=discord.Color.purple())
    await i.response.send_message(embed=embed)

@bot.tree.command(name="girar-roleta", description="🎰 Roleta russa")
async def roleta(i: discord.Interaction):
    resultado = "💥 BOOM!" if random.random() < 0.16 else "😅 Escapou!"
    await i.response.send_message(f"🎰 {resultado}")

@bot.tree.command(name="quiz", description="🧠 Quiz")
async def quiz(i: discord.Interaction):
    q = random.choice(QUIZZES)
    embed = discord.Embed(title="🧠", description=q["pergunta"], color=discord.Color.blue())
    for idx, opcao in enumerate(q["opcoes"]):
        embed.add_field(name=chr(65 + idx), value=opcao, inline=True)
    view = QuizView(q["opcoes"], q["correta"])
    await i.response.send_message(embed=embed, view=view)

# ================= COMANDOS MODERAÇÃO =================
@bot.tree.command(name="warn", description="⚠️ Avisar membro")
@app_commands.default_permissions(manage_messages=True)
async def warn(i: discord.Interaction, membro: discord.Member, motivo: str):
    await db.add_warn(membro.id, i.guild_id, i.user.name, motivo)
    warns = await db.get_recent_warns(membro.id, i.guild_id)
    
    await i.response.send_message(f"⚠️ {membro.mention} - Total: {warns}")
    
    embed = discord.Embed(title="⚠️ Membro Advertido", color=discord.Color.orange())
    embed.add_field(name="Membro", value=membro.mention)
    embed.add_field(name="Moderador", value=i.user.mention)
    embed.add_field(name="Motivo", value=motivo, inline=False)
    embed.add_field(name="Total de Avisos", value=warns)
    await enviar_log(i.guild, embed)

@bot.tree.command(name="warns", description="⚠️ Ver avisos")
@app_commands.default_permissions(manage_messages=True)
async def check_warns(i: discord.Interaction, membro: discord.Member):
    avisos = await db.get_warns(membro.id, i.guild_id)
    embed = discord.Embed(title=f"⚠️ Avisos de {membro.name}", color=discord.Color.orange())
    if avisos:
        for aviso in avisos[-5:]:
            embed.add_field(name=f"Aviso de {aviso['staff_name']}", value=aviso['reason'], inline=False)
    else:
        embed.description = "Sem avisos"
    await i.response.send_message(embed=embed)

@bot.tree.command(name="mute", description="🔇 Silenciar membro")
@app_commands.default_permissions(manage_messages=True)
async def mute(i: discord.Interaction, membro: discord.Member, minutos: int):
    await membro.timeout(timedelta(minutes=minutos))
    await i.response.send_message(f"🔇 {membro.mention} silenciado por {minutos}m")
    
    embed = discord.Embed(title="🔇 Mute", color=discord.Color.orange())
    embed.add_field(name="Membro", value=membro.mention)
    embed.add_field(name="Moderador", value=i.user.mention)
    embed.add_field(name="Tempo", value=f"{minutos} minutos")
    await enviar_log(i.guild, embed)

@bot.tree.command(name="kick", description="👢 Expulsar membro")
@app_commands.default_permissions(kick_members=True)
async def kick(i: discord.Interaction, membro: discord.Member):
    await membro.kick()
    await i.response.send_message(f"👢 {membro.mention} expulso")
    
    embed = discord.Embed(title="👢 Kick", color=discord.Color.red())
    embed.add_field(name="Membro", value=membro.mention)
    embed.add_field(name="Moderador", value=i.user.mention)
    await enviar_log(i.guild, embed)

@bot.tree.command(name="ban", description="🔨 Banir membro")
@app_commands.default_permissions(ban_members=True)
async def ban(i: discord.Interaction, membro: discord.Member):
    await membro.ban()
    await i.response.send_message(f"🔨 {membro.mention} banido")
    
    embed = discord.Embed(title="🔨 Ban", color=discord.Color.red())
    embed.add_field(name="Membro", value=membro.mention)
    embed.add_field(name="Moderador", value=i.user.mention)
    await enviar_log(i.guild, embed)

@bot.tree.command(name="purge", description="🧹 Limpar mensagens")
@app_commands.default_permissions(manage_messages=True)
async def purge(i: discord.Interaction, qtd: int):
    apagadas = await i.channel.purge(limit=qtd)
    await i.response.send_message(f"🧹 {len(apagadas)} mensagens apagadas", ephemeral=True)

@bot.tree.command(name="lockall", description="🔒 Travar todos canais")
@app_commands.default_permissions(administrator=True)
async def lockall(i: discord.Interaction):
    await i.response.defer(ephemeral=True)
    count = 0
    for ch in i.guild.text_channels:
        try:
            await ch.set_permissions(i.guild.default_role, send_messages=False)
            count += 1
        except:
            pass
    await i.followup.send(f"🔒 {count} canais travados!", ephemeral=True)

@bot.tree.command(name="unlockall", description="🔓 Destravar todos canais")
@app_commands.default_permissions(administrator=True)
async def unlockall(i: discord.Interaction):
    await i.response.defer(ephemeral=True)
    count = 0
    for ch in i.guild.text_channels:
        try:
            await ch.set_permissions(i.guild.default_role, send_messages=True)
            count += 1
        except:
            pass
    await i.followup.send(f"🔓 {count} canais destravados!", ephemeral=True)

@bot.tree.command(name="limpar-warns", description="🗑️ Limpar avisos")
@app_commands.default_permissions(administrator=True)
async def limpar_warns(i: discord.Interaction, membro: discord.Member):
    await db.clear_warns(membro.id, i.guild_id)
    await i.response.send_message(f"🗑️ Avisos de {membro.mention} limpos!", ephemeral=True)

@bot.tree.command(name="historico", description="📋 Histórico completo")
@app_commands.default_permissions(manage_messages=True)
async def historico(i: discord.Interaction, membro: discord.Member):
    avisos = await db.get_warns(membro.id, i.guild_id)
    embed = discord.Embed(title=f"📋 Histórico de {membro.name}", color=discord.Color.blue())
    embed.add_field(name="Total de Avisos", value=len(avisos))
    if avisos:
        for aviso in avisos[-5:]:
            data = datetime.fromtimestamp(aviso['timestamp']).strftime("%d/%m")
            embed.add_field(name=f"{aviso['staff_name']} - {data}", value=aviso['reason'], inline=False)
    await i.response.send_message(embed=embed)

# ================= COMANDOS SETUP =================
@bot.tree.command(name="setup", description="⚙️ Configurar canal")
@app_commands.default_permissions(administrator=True)
async def setup(i: discord.Interaction, canal: discord.TextChannel, tipo: str):
    if tipo == "logs":
        await db.update_guild_config(i.guild_id, logs_channel=canal.id)
    elif tipo == "welcome":
        await db.update_guild_config(i.guild_id, welcome_channel=canal.id)
    elif tipo == "goodbye":
        await db.update_guild_config(i.guild_id, goodbye_channel=canal.id)
    
    await i.response.send_message(f"✅ Canal {tipo} configurado em {canal.mention}!", ephemeral=True)

@bot.tree.command(name="config-show", description="📊 Ver configurações")
@app_commands.default_permissions(administrator=True)
async def config_show(i: discord.Interaction):
    config = await db.get_guild_config(i.guild_id)
    embed = discord.Embed(title="📊 Configurações", color=discord.Color.gold())
    if config:
        logs = i.guild.get_channel(config['logs_channel']) if config['logs_channel'] else None
        welcome = i.guild.get_channel(config['welcome_channel']) if config['welcome_channel'] else None
        embed.add_field(name="📋 Logs", value=logs.mention if logs else "❌")
        embed.add_field(name="👋 Welcome", value=welcome.mention if welcome else "❌")
    else:
        embed.description = "Nenhuma configuração. Use `/setup`"
    await i.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="resetar-config", description="🔄 Resetar configurações")
@app_commands.default_permissions(administrator=True)
async def resetar(i: discord.Interaction):
    await db.update_guild_config(i.guild_id, logs_channel=None, welcome_channel=None)
    await i.response.send_message("🔄 Configurações resetadas!", ephemeral=True)

@bot.tree.command(name="raid-logs", description="📋 Logs de segurança")
@app_commands.default_permissions(administrator=True)
async def raid_logs(i: discord.Interaction):
    raids = await db.get_raids(i.guild_id, limit=10)
    embed = discord.Embed(title="📋 Logs de Segurança", color=discord.Color.red())
    
    if raids:
        for raid in raids:
            data = datetime.fromtimestamp(raid['timestamp']).strftime("%d/%m %H:%M")
            embed.add_field(name=f"[{data}] {raid['tipo']}", value=raid['detalhes'], inline=False)
    else:
        embed.description = "✅ Nenhum incidente!"
    
    await i.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="raid-status", description="🚨 Status de raids")
@app_commands.default_permissions(administrator=True)
async def raid_status(i: discord.Interaction):
    raids = await db.get_raids(i.guild_id, limit=5)
    embed = discord.Embed(title="🚨 Status de Raids", color=discord.Color.red())
    if raids:
        for raid in raids:
            data = datetime.fromtimestamp(raid['timestamp']).strftime("%d/%m %H:%M")
            embed.add_field(name=f"{raid['tipo']} - {data}", value=raid['detalhes'], inline=False)
    else:
        embed.description = "✅ Nenhum raid detectado!"
    await i.response.send_message(embed=embed, ephemeral=True)

# ================= COMANDOS DIVERSÃO =================
@bot.tree.command(name="sorteio", description="🎉 Fazer sorteio")
@app_commands.default_permissions(administrator=True)
async def sorteio(i: discord.Interaction, premio: str, minutos: int):
    embed = discord.Embed(title="🎉 Sorteio!", description=f"**{premio}**", color=discord.Color.gold())
    view = SorteioView(premio)
    await i.response.send_message(embed=embed, view=view)
    msg = await i.original_response()
    await asyncio.sleep(minutos * 60)
    if view.participantes:
        vencedor = random.choice(list(view.participantes))
        await db.add_coins(vencedor, 500)
        embed_fim = discord.Embed(title="🎉 FIM!", description=f"🏆 <@{vencedor}>!", color=discord.Color.green())
        await msg.edit(embed=embed_fim, view=None)
    else:
        embed_fim = discord.Embed(title="❌ Sorteio Cancelado", color=discord.Color.red())
        await msg.edit(embed=embed_fim, view=None)

@bot.tree.command(name="enquete", description="📊 Fazer enquete")
async def enquete(i: discord.Interaction, pergunta: str):
    embed = discord.Embed(title="📊", description=pergunta, color=discord.Color.blue())
    msg = await i.response.send_message(embed=embed)
    await msg.add_reaction("👍")
    await msg.add_reaction("👎")

@bot.tree.command(name="sugerir", description="💡 Sugerir algo")
async def sugerir(i: discord.Interaction, sugestao: str):
    await db.add_sugestao(i.user.id, i.guild_id, sugestao)
    embed = discord.Embed(title="💡 Sugestão Recebida", description=sugestao, color=discord.Color.green())
    await i.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="sugestoes", description="📝 Ver sugestões")
async def sugestoes(i: discord.Interaction):
    sug = await db.get_sugestoes(i.guild_id)
    embed = discord.Embed(title="📝 Sugestões", color=discord.Color.blue())
    if sug:
        for s in sug[-5:]:
            try:
                user = await bot.fetch_user(s['user_id'])
                embed.add_field(name=f"De {user.name}", value=s['sugestao'], inline=False)
            except:
                pass
    else:
        embed.description = "Sem sugestões"
    await i.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="tag", description="🏷️ Sistema de tags")
async def tag_cmd(i: discord.Interaction, acao: str, nome: str, conteudo: str = None):
    if acao.lower() == "criar":
        await db.add_tag(i.guild_id, nome, conteudo, i.user.id)
        await i.response.send_message(f"✅ Tag `{nome}` criada!", ephemeral=True)
    elif acao.lower() == "chamar":
        tag = await db.get_tag(i.guild_id, nome)
        if tag:
            await i.response.send_message(tag['conteudo'])
        else:
            await i.response.send_message(f"❌ Tag `{nome}` não existe", ephemeral=True)
    elif acao.lower() == "listar":
        tags = await db.get_tags(i.guild_id)
        embed = discord.Embed(title="🏷️ Tags", color=discord.Color.blue())
        for tag in tags:
            embed.add_field(name=tag['nome'], value=f"De <@{tag['criador_id']}>", inline=True)
        await i.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="lembrar", description="🔔 Definir lembrete")
async def lembrar(i: discord.Interaction, mensagem: str, tempo: str):
    try:
        if "h" in tempo:
            segundos = int(tempo.replace("h", "")) * 3600
        elif "m" in tempo:
            segundos = int(tempo.replace("m", "")) * 60
        else:
            segundos = int(tempo)
        await db.add_lembrete(i.user.id, mensagem, segundos)
        await i.response.send_message(f"🔔 Lembrete em {tempo}!", ephemeral=True)
    except:
        await i.response.send_message("❌ Formato: `/lembrar msg 1h` ou `/lembrar msg 30m`", ephemeral=True)

@bot.tree.command(name="meus-lembretes", description="📋 Meus lembretes")
async def meus_lembretes(i: discord.Interaction):
    lembretes = await db.get_lembretes(i.user.id)
    embed = discord.Embed(title="📋 Lembretes", color=discord.Color.blue())
    if lembretes:
        for l in lembretes:
            embed.add_field(name=l['mensagem'], value=f"Em {int(l['tempo_restante'])}s", inline=True)
    else:
        embed.description = "Sem lembretes"
    await i.response.send_message(embed=embed, ephemeral=True)

@bot.tree.error
async def erro(i: discord.Interaction, error: Exception):
    logger.error(f"Erro: {error}")
    try:
        if not i.response.is_done():
            await i.response.send_message("❌ Erro ao processar comando", ephemeral=True)
    except:
        pass


# Servidor HTTP fake para manter o Render feliz no plano grátis
import asyncio
from aiohttp import web

async def handle(request):
    return web.Response(text="Bot online!")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

async def main():
    # Sobe o servidor web em segundo plano
    await start_web_server()
    # Inicia o bot do Discord
    async with bot:
        await bot.start(TOKEN)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Erro: {e}")
