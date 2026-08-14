#!/usr/bin/env python3
"""Executor — orchestration seule, service partagé Structory (ARCHITECTURE.md §4 du projet
Suivre Mes Comptes, révisé 2026-07-20 : ce n'est pas un composant propre à SMC). Ne fait
jamais de logique métier ni de stockage propre : liste les Comptes d'une organisation,
résout leurs connectors compatibles via Analyzor, appelle chaque connector, transmet le
solde obtenu à ledger_api. SMC est son premier appelant, pas son seul.
"""
import json
import os
import time
import unicodedata
import uuid
from datetime import datetime, timedelta
from urllib.parse import quote

from flask import Flask, jsonify, redirect, request
import requests

app = Flask(__name__)

ANALYZOR_URL = os.environ.get('ANALYZOR_URL', 'http://localhost:8000')
LEDGER_API_URL = os.environ.get('LEDGER_API_URL', 'http://localhost:8080')
SUBSCRIPTIONS_API_URL = os.environ.get('SUBSCRIPTIONS_API_URL', 'http://localhost:8082')
SUBSCRIPTIONS_SERVICE_KEY = os.environ.get('SUBSCRIPTIONS_SERVICE_KEY', '***REMOVED_SERVICE_KEY***')
# Lien "Ouvrir Suivre Mes Comptes" dans l'email quotidien (2026-07-26) — même Navigator pour
# toute org, jamais une URL par org codée en dur (?orgId= ajouté dynamiquement à l'envoi).
NAVIGATOR_URL = os.environ.get('NAVIGATOR_URL', 'https://script.google.com/macros/s/AKfycbzJ_mGTi4mYSVAMBZIWJ1ybbEaDyOaF6AGrzZo-VU8mv7jp5n5YzE2vCJcCz4JBX3TEkQ/exec')
# URL publique de CE service (VPS, pas de reverse proxy/domaine configuré) — nécessaire comme
# redirect_url pour le flux d'autorisation Enable Banking (2026-07-27) : le navigateur du PSU
# doit pouvoir l'atteindre directement, contrairement à ANALYZOR_URL/LEDGER_API_URL qui restent
# des appels serveur-à-serveur en localhost.
EXECUTOR_PUBLIC_URL = os.environ.get('EXECUTOR_PUBLIC_URL', 'http://213.32.16.118:8084')


def log_to_journal(org_id, actor, summary, details=None):
    """Même pattern fail-open que ledger_api/app.py : une panne du journal technique ne doit
    jamais faire échouer une synchronisation réelle."""
    try:
        requests.post(
            f'{ANALYZOR_URL}/api/journal/log',
            json={'orgId': org_id, 'actor': actor, 'summary': summary, 'details': details or []},
            timeout=3,
        )
    except requests.RequestException:
        pass


def _safe_upstream_error(org_id, actor, friendly_message, exception):
    """Ne JAMAIS renvoyer le texte brut d'une exception réseau à un utilisateur final (bug réel
    trouvé 2026-07-29, retour de Stéphane : "tu ne dois pas afficher ce genre de choses" —
    l'erreur brute d'un fournisseur tiers, page HTML complète + Request ID compris, remontait
    telle quelle jusqu'à l'écran de recherche de banque). Le détail réel part dans le journal
    technique (fail-open, jamais bloquant) pour le débogage ; l'utilisateur ne voit QUE
    `friendly_message`."""
    log_to_journal(org_id or '?', actor, 'Erreur upstream (détail interne)', [str(exception)])
    return jsonify({'success': False, 'error': friendly_message}), 502


def _org_secret_value(org_id, name):
    """Secret chiffré d'une org (org_secrets.py), lu via la route strictement interne
    d'Analyzor — même mécanisme que _org_smtp_secret, généralisé pour tout connector qui a
    besoin d'une clé API (2026-07-25, 1er connector réel : Mercury)."""
    r = requests.get(
        f'{ANALYZOR_URL}/api/org/{org_id}/secrets/{name}/value',
        headers={'X-Service-Key': SUBSCRIPTIONS_SERVICE_KEY}, timeout=10,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()['value']


# Registre des connectors d'entrée réellement implémentés : interface (contenu.interface
# d'une brique connector, §6) -> {'fetch': fonction, 'secret_name': nom du secret org requis
# (ou None si le connector n'en a pas besoin)}. `fetch(compte_brick)` ou
# `fetch(compte_brick, secret_value)` selon que `secret_name` est renseigné. Un compte dont le
# connector n'est pas dans ce registre échoue proprement (voir _sync_one_compte), il ne
# bloque jamais les autres comptes.
#
# `secret_name_fn` (au lieu de `secret_name` fixe) : nécessaire pour Qonto, où une clé API
# authentifie UNE SEULE organisation Qonto (pas un accès multi-comptes comme Mercury) — "Ferme
# Verte 323" et "Ferme Verte Photovoltaïque" sont deux organisations Qonto distinctes avec deux
# credentials séparés (confirmé 2026-07-25 par Stéphane, captures d'écran du panneau API Qonto
# des deux comptes). Le nom du secret est donc dérivé du `titulaire` de la brique Compte plutôt
# que fixe.
import re

import connector_enablebanking
import connector_mercury
import connector_powens
import connector_qonto

from core.connector import Connector, STATUS_ACTIVE, STATUS_UNCONFIGURED
from core.flow import Flow


def _slug(s):
    return re.sub(r'[^a-z0-9]+', '_', (s or '').lower()).strip('_')


def _qonto_secret_name(compte_brick):
    titulaire = compte_brick.get('contenu', {}).get('titulaire') or compte_brick.get('contenu', {}).get('nom') or ''
    return f'qonto_api_key_{_slug(titulaire)}'


CONNECTOR_REGISTRY = {
    'connector_mercury': {'fetch': connector_mercury.fetch, 'secret_name': 'mercury_api_key'},
    'connector_qonto': {'fetch': connector_qonto.fetch, 'secret_name_fn': _qonto_secret_name},
    # Un seul secret partagé pour toute l'org : contrairement à Qonto, l'app_id + la clé privée
    # Enable Banking ne sont pas liés à un établissement précis (l'établissement/la banque est
    # déterminé par le consentement PSD2 déjà donné, capturé sur `enablebanking_account_uid` de
    # chaque brique Compte, voir connector_enablebanking.py).
    # `fetch_transactions` (2026-08-14, JournaldeBanque) : historique détaillé, en plus du solde
    # (`fetch`) — même secret, mêmes credentials, juste un autre endpoint upstream. Absent du
    # registre pour Mercury/Qonto (pas encore construit pour eux, seulement Powens/Enable
    # Banking pour cette V0 — voir ~/projects/jdb/CLAUDE.md) : `_fetch_transactions_for_compte`
    # gère cette absence proprement (erreur claire, jamais un crash).
    'connector_enablebanking': {
        'fetch': connector_enablebanking.fetch,
        'fetch_transactions': connector_enablebanking.fetch_transactions,
        'secret_name': 'enablebanking_credentials',
    },
    # Idem : un seul secret partagé pour toute l'org (l'établissement est déterminé par la
    # connexion Powens déjà établie, capturée sur `powens_account_id` de chaque brique Compte).
    'connector_powens': {
        'fetch': connector_powens.fetch,
        'fetch_transactions': connector_powens.fetch_transactions,
        'secret_name': 'powens_credentials',
    },
}


def _sync_one_compte(org_id, module, compte_brick):
    contenu = compte_brick.get('contenu', {})
    nom = contenu.get('nom') or compte_brick.get('title') or compte_brick.get('id')
    etablissement = contenu.get('etablissement')
    nature = contenu.get('nature')
    titulaire = contenu.get('titulaire')
    produit = contenu.get('produit')
    devise = contenu.get('devise_origine') or 'EUR'

    if not etablissement or not nature:
        return {'success': False, 'compte': nom, 'error': 'etablissement/nature manquant sur la brique Compte'}

    try:
        r = requests.get(
            f'{ANALYZOR_URL}/api/connectors/resolve',
            params={'etablissement': etablissement, 'nature': nature, 'orgId': org_id, 'module': module},
            timeout=30,  # scan Drive à froid ~24s avant mise en cache côté Analyzor (2026-07-25)
        )
        r.raise_for_status()
        connectors = r.json().get('connectors', [])
    except requests.RequestException as e:
        return {'success': False, 'compte': nom, 'error': f'Analyzor injoignable (résolution connector) : {e}'}

    if not connectors:
        return {'success': False, 'compte': nom, 'error': f"Aucun connector résolu pour {etablissement}/{nature}"}

    interface = connectors[0]['interface']
    entry = CONNECTOR_REGISTRY.get(interface)
    if entry is None:
        return {'success': False, 'compte': nom, 'error': f"Connector '{interface}' pas encore implémenté"}

    try:
        secret_name = entry.get('secret_name') or (entry['secret_name_fn'](compte_brick) if entry.get('secret_name_fn') else None)
        if secret_name:
            secret = _org_secret_value(org_id, secret_name)
            if secret is None:
                return {'success': False, 'compte': nom, 'error': f"Clé API '{secret_name}' pas encore configurée pour cette org"}
            soldes = entry['fetch'](compte_brick, secret)
        else:
            soldes = entry['fetch'](compte_brick)
    except Exception as e:
        return {'success': False, 'compte': nom, 'error': f'Connector {interface} en échec : {e}'}

    posted = []
    for point in soldes:
        try:
            r = requests.post(f'{LEDGER_API_URL}/api/ledger/balance-point', json={
                'orgId': org_id,
                'etablissement': etablissement,
                'nature': nature,
                'titulaire': titulaire,
                'produit': produit,
                'solde': point['solde'],
                'devise': point.get('devise', devise),
                'date': point.get('date'),
            }, timeout=10)
            r.raise_for_status()
            posted.append(r.json())
        except requests.RequestException as e:
            return {'success': False, 'compte': nom, 'error': f'ledger_api injoignable : {e}'}

    compte_ledger = posted[0]['compte'] if posted else None
    return {'success': True, 'compte': nom, 'compteLedger': compte_ledger, 'points': posted}


def _sync_org_comptes(org_id, module):
    """Synchronise tous les Comptes d'une organisation : résout leur connector, récupère un
    solde normalisé, le transmet à /api/ledger/balance-point. Ne s'arrête jamais sur l'échec
    d'un compte : chaque compte est indépendant, un incident isolé (connector pas encore
    implémenté, service injoignable) ne doit pas empêcher les autres de se synchroniser.
    Factorisé (2026-07-26) : appelé par la route /api/executor/sync ET par daily_report avant
    de calculer le patrimoine, pour que l'email quotidien reflète des soldes fraîchement
    synchronisés plutôt que la dernière valeur connue (retour de Stéphane : "il faut bien sûr
    automatiser avant l'envoi d'email")."""
    r = requests.get(f'{ANALYZOR_URL}/api/org/{org_id}/bricks', params={'type': 'Compte'}, timeout=30)
    r.raise_for_status()
    comptes = r.json().get('bricks', [])

    results = [_sync_one_compte(org_id, module, c) for c in comptes]
    n_ok = sum(1 for r in results if r['success'])

    log_to_journal(
        org_id, 'executor',
        f'Synchronisation : {n_ok}/{len(results)} comptes OK',
        [f"{r['compte']} : {'OK' if r['success'] else r.get('error')}" for r in results],
    )
    return results


@app.route('/api/executor/sync', methods=['POST'])
def sync():
    """Body: {orgId, module?} — voir _sync_org_comptes."""
    try:
        data = request.get_json() or {}
        org_id = data.get('orgId', '')
        module = data.get('module')

        if not org_id:
            return jsonify({'success': False, 'error': 'orgId manquant'}), 400

        try:
            results = _sync_org_comptes(org_id, module)
        except requests.RequestException as e:
            return jsonify({'success': False, 'error': f'Analyzor injoignable : {e}'}), 502

        return jsonify({'success': True, 'nComptes': len(results), 'results': results})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


def _matches(compte_brick, etablissement, nature, titulaire, produit):
    """Égalité stricte sur les 4 champs (2026-07-26 : `produit` ajouté après la collision réelle
    "SPL Livret Bleu"/"SPL LDD", mêmes établissement+nature+titulaire). Chaîne vide et None
    traités pareil des deux côtés — un appelant qui ne connaît pas `produit`/`titulaire` doit
    matcher un compte qui n'en a pas non plus, jamais "n'importe lequel" (bug corrigé au passage
    : l'ancienne comparaison ignorait silencieusement `titulaire` quand l'appelant ne le
    fournissait pas, risquant de sélectionner le mauvais compte parmi plusieurs)."""
    c = compte_brick.get('contenu', {})
    return (
        (c.get('etablissement') or '') == (etablissement or '')
        and (c.get('nature') or '') == (nature or '')
        and (c.get('titulaire') or '') == (titulaire or '')
        and (c.get('produit') or '') == (produit or '')
    )


@app.route('/api/executor/sync-one', methods=['POST'])
def sync_one():
    """Synchronise UN seul compte (vue patrimoine Navigator, 2026-07-26 : bouton "lancer une
    synchronisation" dans le panneau latéral d'un compte précis) — même logique que `sync()`
    mais sans boucler sur toute l'organisation. Body: {orgId, module?, etablissement, nature,
    titulaire?, produit?}"""
    try:
        data = request.get_json() or {}
        org_id = data.get('orgId', '')
        module = data.get('module')
        etablissement = data.get('etablissement', '')
        nature = data.get('nature', '')
        titulaire = data.get('titulaire')
        produit = data.get('produit')
        if not org_id or not etablissement or not nature:
            return jsonify({'success': False, 'error': 'orgId, etablissement, nature requis'}), 400

        try:
            r = requests.get(f'{ANALYZOR_URL}/api/org/{org_id}/bricks', params={'type': 'Compte'}, timeout=30)
            r.raise_for_status()
            comptes = r.json().get('bricks', [])
        except requests.RequestException as e:
            return jsonify({'success': False, 'error': f'Analyzor injoignable : {e}'}), 502

        compte_brick = next((c for c in comptes if _matches(c, etablissement, nature, titulaire, produit)), None)
        if compte_brick is None:
            return jsonify({'success': False, 'error': 'Compte introuvable pour ces critères'}), 404

        result = _sync_one_compte(org_id, module, compte_brick)
        return jsonify(result)

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================
# JOURNALDEBANQUE — historique détaillé des transactions (2026-08-14), en plus du solde
# (`_sync_one_compte` ci-dessus). Consommé par `jdb_api` (staging des propositions), jamais par
# l'utilisateur/Navigator directement (§4 : "proposition soumise à l'organisation").
# ============================================================

def _fetch_transactions_for_compte(org_id, module, compte_brick, sanctuarize=True):
    """Résout le connector d'un Compte déjà lié (même cascade que `_sync_one_compte`) et
    renvoie ses transactions normalisées. Écrit AUSSI, dans le même mouvement, un snapshot
    sanctuarisé dans l'Own Storage de l'org (§2bis) — jamais bloquant : un relevé qui échoue à
    s'écrire (org pas encore bootstrapée, voir own_storage_releves.py) n'empêche jamais
    l'appelant (jdb_api) de recevoir les transactions déjà récupérées, juste signalé dans le
    résultat (`sanctuarise.success=False`)."""
    contenu = compte_brick.get('contenu', {})
    nom = contenu.get('nom') or compte_brick.get('title') or compte_brick.get('id')
    etablissement = contenu.get('etablissement')
    nature = contenu.get('nature')
    titulaire = contenu.get('titulaire')
    produit = contenu.get('produit')

    if not etablissement or not nature:
        return {'success': False, 'compte': nom, 'error': 'etablissement/nature manquant sur la brique Compte'}

    try:
        r = requests.get(
            f'{ANALYZOR_URL}/api/connectors/resolve',
            params={'etablissement': etablissement, 'nature': nature, 'orgId': org_id, 'module': module},
            timeout=30,
        )
        r.raise_for_status()
        connectors = r.json().get('connectors', [])
    except requests.RequestException as e:
        return {'success': False, 'compte': nom, 'error': f'Analyzor injoignable (résolution connector) : {e}'}

    if not connectors:
        return {'success': False, 'compte': nom, 'error': f"Aucun connector résolu pour {etablissement}/{nature}"}

    interface = connectors[0]['interface']
    entry = CONNECTOR_REGISTRY.get(interface)
    if entry is None or 'fetch_transactions' not in entry:
        # §3 : signalé comme compte non automatisable pour l'historique détaillé — prioritairement
        # affiché dans JdB (jdb_api relaie cette erreur telle quelle dans son statut de compte),
        # un email n'étant qu'un complément (jamais construit ici, hors périmètre d'un connector).
        return {'success': False, 'compte': nom, 'error': f"Connector '{interface}' ne fournit pas encore d'historique de transactions (fetch_transactions)"}

    try:
        secret_name = entry.get('secret_name') or (entry['secret_name_fn'](compte_brick) if entry.get('secret_name_fn') else None)
        if not secret_name:
            return {'success': False, 'compte': nom, 'error': f"Connector '{interface}' sans secret_name configuré pour les transactions"}
        secret = _org_secret_value(org_id, secret_name)
        if secret is None:
            return {'success': False, 'compte': nom, 'error': f"Clé API '{secret_name}' pas encore configurée pour cette org"}
        transactions = entry['fetch_transactions'](compte_brick, secret)
    except Exception as e:
        return {'success': False, 'compte': nom, 'error': f'Connector {interface} en échec : {e}'}

    raw_payload = []
    normalized = []
    for t in transactions:
        t = dict(t)
        raw_payload.append(t.pop('_raw', None))
        normalized.append(t)

    sanctuarize_result = {'success': False, 'skipped': True}
    if sanctuarize and normalized:
        releve_name = _slug(f"{interface}_{etablissement}_{titulaire or ''}_{nature}") + '.jsonl'
        try:
            resp = requests.post(f'{ANALYZOR_URL}/api/ownstorage/releve/append', json={
                'orgId': org_id,
                'name': releve_name,
                'record': {
                    'connector': interface,
                    'compte': {'etablissement': etablissement, 'nature': nature, 'titulaire': titulaire, 'produit': produit},
                    'n_transactions': len(normalized),
                    'source_ids': [t.get('source_id') for t in normalized],
                    'raw': raw_payload,
                },
            }, timeout=20)
            sanctuarize_result = resp.json()
            sanctuarize_result.setdefault('httpStatus', resp.status_code)
        except requests.RequestException as e:
            sanctuarize_result = {'success': False, 'error': f'Analyzor injoignable (sanctuarisation) : {e}'}

    return {
        'success': True, 'compte': nom,
        'etablissement': etablissement, 'nature': nature, 'titulaire': titulaire, 'produit': produit,
        'transactions': normalized, 'sanctuarise': sanctuarize_result,
    }


@app.route('/api/executor/fetch-transactions', methods=['POST'])
def fetch_transactions_route():
    """Historique détaillé des transactions d'UN compte déjà lié (JournaldeBanque). Body:
    {orgId, module?, etablissement, nature, titulaire?, produit?, sanctuarize?: bool (défaut
    true)} — même forme de requête que `/api/executor/sync-one`."""
    try:
        data = request.get_json() or {}
        org_id = data.get('orgId', '')
        module = data.get('module')
        etablissement = data.get('etablissement', '')
        nature = data.get('nature', '')
        titulaire = data.get('titulaire')
        produit = data.get('produit')
        sanctuarize = data.get('sanctuarize', True)
        if not org_id or not etablissement or not nature:
            return jsonify({'success': False, 'error': 'orgId, etablissement, nature requis'}), 400

        try:
            r = requests.get(f'{ANALYZOR_URL}/api/org/{org_id}/bricks', params={'type': 'Compte'}, timeout=30)
            r.raise_for_status()
            comptes = r.json().get('bricks', [])
        except requests.RequestException as e:
            return jsonify({'success': False, 'error': f'Analyzor injoignable : {e}'}), 502

        compte_brick = next((c for c in comptes if _matches(c, etablissement, nature, titulaire, produit)), None)
        if compte_brick is None:
            return jsonify({'success': False, 'error': 'Compte introuvable pour ces critères'}), 404

        result = _fetch_transactions_for_compte(org_id, module, compte_brick, sanctuarize=sanctuarize)
        return jsonify(result)

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ============================================================
# ENABLE BANKING — liaison d'un nouveau compte (2026-07-27, retour de Stéphane : "le fondement
# de ce programme" — jusqu'ici seul fetch() [solde d'un compte DÉJÀ lié] existait, tout le flux
# d'autorisation initiale avait été fait une fois à la main hors service pour le vrai Crédit
# Mutuel). État `_eb_pending` en mémoire (process unique, un redémarrage perd les liaisons en
# cours — acceptable, un flux d'autorisation dure quelques minutes, jamais des heures) :
# state -> {orgId, email, expiresAt, accounts?}. `accounts` rempli seulement après le callback.
# ============================================================
_EB_PENDING_TTL_SECONDS = 15 * 60
_eb_pending = {}


def _eb_credentials(org_id):
    """Retourne (credentials, mode) — mode='production' si l'org a configuré SES PROPRES
    identifiants Enable Banking de production POUR LA LIAISON DE NOUVEAUX COMPTES (secret
    `enablebanking_selfservice_credentials`, via le formulaire self-service "Connecteurs" du
    panneau d'organisation, 2026-07-28 : "chaque user grâce à son profil dans son organisation
    crée son outil qui lui permettra de voir ses comptes" — Stéphane), sinon repli sur
    `enablebanking_sandbox_credentials` (mode='sandbox', ne peut connecter QUE des banques
    factices).

    Nom de secret DÉLIBÉRÉMENT distinct de `enablebanking_credentials` (bug réel trouvé et
    corrigé le 2026-07-28 : réutiliser ce même nom cassait smcspl, qui a DÉJÀ un
    `enablebanking_credentials` — l'app de production PARTAGÉE utilisée par
    CONNECTOR_REGISTRY/fetch() pour synchroniser Crédit Mutuel, `Restricted` côté Enable Banking,
    qui ne peut PAS lier de nouveaux comptes). Les deux secrets peuvent coexister pour une même
    org : `enablebanking_credentials` sert UNIQUEMENT à synchroniser des comptes déjà liés,
    `enablebanking_selfservice_credentials` sert UNIQUEMENT à en lier de nouveaux — deux apps
    Enable Banking différentes, deux usages différents, même si toutes deux "de production".

    Repli sur `enablebanking_credentials` en dernier recours (mode='restricted', 2026-07-29,
    retour de Stéphane : la recherche de banque ne trouvait pas "Crédit Mutuel" alors que
    l'organisation a DÉJÀ un vrai accès Enable Banking pour ce compte — vérifié : `/aspsps` avec
    CES identifiants renvoie bien un catalogue complet, 2632 banques, Crédit Mutuel inclus, la
    restriction ne bloque QUE la liaison de nouveaux comptes, pas la lecture du catalogue). Une
    tentative de liaison avec ce mode peut échouer côté Enable Banking (app Restricted) — c'est
    alors Enable Banking lui-même qui le dira, jamais caché ou deviné à l'avance côté recherche."""
    # Ordre de priorité : production self-service (peut tout faire) > restricted (vrai
    # catalogue + tentative de liaison réelle, même si elle peut échouer) > sandbox (dernier
    # recours, jamais de vraie banque). Bug réel trouvé le 2026-07-29 : 'sandbox' était vérifié
    # AVANT 'restricted' — smcspl a les deux secrets configurés, donc 'sandbox' gagnait toujours
    # et la recherche ne voyait jamais le vrai catalogue Crédit Mutuel de l'app restricted.
    raw = _org_secret_value(org_id, 'enablebanking_selfservice_credentials')
    if raw:
        return json.loads(raw), 'production'
    raw = _org_secret_value(org_id, 'enablebanking_credentials')
    if raw:
        return json.loads(raw), 'restricted'
    raw = _org_secret_value(org_id, 'enablebanking_sandbox_credentials')
    if raw:
        return json.loads(raw), 'sandbox'
    return None, None


@app.route('/api/executor/enablebanking/start-auth', methods=['POST'])
def enablebanking_start_auth():
    """Démarre une liaison de compte Enable Banking pour un user de l'org. Body: {orgId, email,
    aspspName?, aspspCountry?}.

    Mode sandbox (pas d'identifiants de production configurés pour cette org) : aspsp TOUJOURS
    forcé à Mock ASPSP, quoi que le client envoie — jamais de vraie banque par erreur avec une
    app qui ne peut de toute façon pas s'y connecter (2026-07-27).

    Mode production (l'org a configuré SES PROPRES identifiants via le panneau "Connecteurs",
    2026-07-28 : "chaque user grâce à son profil dans son organisation crée son outil qui lui
    permettra de voir ses comptes") : `aspspName`/`aspspCountry` requis — impossible de deviner
    quelle banque réelle l'utilisateur veut connecter."""
    org_id = ''
    try:
        data = request.get_json() or {}
        org_id = data.get('orgId', '')
        email = (data.get('email') or '').strip()
        if not org_id or not email:
            return jsonify({'success': False, 'error': 'orgId et email requis'}), 400

        credentials, mode = _eb_credentials(org_id)
        if not credentials:
            return jsonify({'success': False, 'error': 'Aucun identifiant Enable Banking configuré pour cette organisation'}), 404

        if mode in ('production', 'restricted'):
            # 'restricted' (2026-07-29) : l'app existante (Crédit Mutuel) peut refuser de lier
            # un NOUVEAU compte — mais on tente quand même avec le vrai nom de banque plutôt que
            # de le deviner/cacher à l'avance : si Enable Banking refuse, il le dira lui-même
            # (message clair renvoyé via _safe_upstream_error), jamais une réponse inventée ici.
            aspsp_name = data.get('aspspName')
            if not aspsp_name:
                return jsonify({'success': False, 'error': 'Nom de la banque requis'}), 400
            aspsp_country = data.get('aspspCountry') or 'FR'
        else:
            aspsp_name = 'Mock ASPSP'
            aspsp_country = 'FI'

        state = uuid.uuid4().hex
        _eb_pending[state] = {'orgId': org_id, 'email': email, 'expiresAt': time.time() + _EB_PENDING_TTL_SECONDS}

        redirect_url = f'{EXECUTOR_PUBLIC_URL}/api/executor/enablebanking/callback'
        url = connector_enablebanking.start_auth(
            credentials,
            aspsp_name=aspsp_name,
            aspsp_country=aspsp_country,
            psu_id=email,
            redirect_url=redirect_url,
            state=state,
        )
        return jsonify({'success': True, 'url': url, 'mode': mode})

    except requests.RequestException as e:
        return _safe_upstream_error(org_id, 'enablebanking_start_auth',
                                     'Impossible de contacter cette banque pour le moment. Réessaie ou choisis une autre banque.', e)
    except Exception as e:
        return _safe_upstream_error(org_id, 'enablebanking_start_auth',
                                     'Une erreur est survenue. Réessaie plus tard.', e)


@app.route('/api/executor/enablebanking/callback', methods=['GET'])
def enablebanking_callback():
    """Reçoit la redirection du PSU après consentement (?code=...&state=...), échange le code
    contre la liste des comptes autorisés, garde le résultat en mémoire (voir `/pending`), puis
    redirige vers Navigator pour que le user finisse la liaison (choisir quelle fiche Compte
    créer/associer pour chaque compte renvoyé)."""
    code = request.args.get('code', '')
    state = request.args.get('state', '')
    eb_error = request.args.get('error', '')
    pending = _eb_pending.get(state)
    if not pending:
        return 'Lien d\'autorisation invalide ou expiré — relance la liaison depuis Navigator.', 400
    if not code:
        # Enable Banking a redirigé avec une erreur (ex. invalid_grant côté Mock ASPSP) plutôt
        # qu'un code — afficher l'erreur réelle plutôt qu'un message générique trompeur (bug
        # réel trouvé 2026-07-27 : le message générique laissait croire à un problème côté
        # Navigator/state alors que c'était Enable Banking lui-même qui refusait).
        return f'Enable Banking a refusé l\'autorisation : {eb_error or "raison inconnue"}. Relance la liaison depuis Navigator.', 400

    try:
        credentials, _mode = _eb_credentials(pending['orgId'])
        session = connector_enablebanking.exchange_code(credentials, code)
        pending['accounts'] = session.get('accounts', [])
    except Exception as e:
        pending['error'] = str(e)

    return redirect(f'{NAVIGATOR_URL}?orgId={pending["orgId"]}&ebPending={state}')


@app.route('/api/executor/enablebanking/pending', methods=['GET'])
def enablebanking_pending():
    """Comptes renvoyés par Enable Banking pour une liaison en cours (voir callback ci-dessus) —
    consommé par Navigator juste après la redirection pour afficher l'écran de finalisation."""
    state = request.args.get('state', '')
    pending = _eb_pending.get(state)
    if not pending or pending['expiresAt'] < time.time():
        return jsonify({'success': False, 'error': 'Liaison introuvable ou expirée'}), 404
    if pending.get('error'):
        return jsonify({'success': False, 'error': pending['error']})
    return jsonify({'success': True, 'email': pending['email'], 'accounts': pending.get('accounts', [])})


# ============================================================
# POWENS — liaison d'un nouveau compte (2026-07-28, retour de Stéphane après avoir buté sur
# Enable Banking pour BCP : contrairement à Enable Banking en self-service (sandbox, ne peut
# JAMAIS connecter de vraie banque, voir _eb_credentials), Powens connecte de VRAIES données
# malgré le nom "sandbox" du domaine — déjà prouvé avec le vrai compte BCP de Stéphane
# (connector_powens.py). Jusqu'ici seul fetch() [solde d'un compte DÉJÀ lié] existait ; la
# liaison initiale (webview) avait été faite une fois à la main.
#
# Pas de callback serveur automatique (contrairement à Enable Banking) : la console Powens de
# l'app "smc" n'autorise QU'UNE SEULE redirect_uri exacte, déjà fixée à `https://structory.ai/`
# (vérifié en conditions réelles le 2026-07-28 — capture d'écran de la console Powens) —
# notre serveur (pas de HTTPS configuré sur ce VPS pour l'instant) ne peut pas être enregistré
# à sa place. Flux adapté en conséquence, réutilisant EXACTEMENT ce qui marchait déjà lors du
# tout premier test manuel (2026-07-26) : la webview redirige vers `https://structory.ai/
# ?connection_id=...`, l'utilisateur colle ce lien (ou juste le connection_id) dans Navigator,
# qui l'envoie à `/api/executor/powens/link-connection` pour récupérer la liste des comptes —
# aucun état "pending" à conserver côté serveur, l'orgId vient directement de Navigator à chaque
# appel. Si un domaine HTTPS pointant vers ce serveur devient disponible un jour (ex. un
# sous-domaine Cloudflare de structory.ai), ce copier-coller manuel pourra être remplacé par un
# vrai callback automatique — pas construit par anticipation, non demandé.
# URI de redirection unique de la plateforme (reçoit connection_id après la webview) — chaque
# app Powens créée par une org doit configurer EXACTEMENT cette URI côté leur console (voir
# guide OrgPanel.html), sinon Powens refuse la redirection (piège réel vécu le 2026-08-02 :
# Stéphane avait enregistré "http://structory.ai" — sans "s", sans slash final — pour l'app
# smcdemo, qui n'aurait jamais fonctionné tant que ça ne correspond pas caractère pour
# caractère).
POWENS_REDIRECT_URI = 'https://structory.ai/'


def _powens_credentials(org_id):
    """Secret `powens_credentials` : {"domain": "...", "auth_token": "...", "client_id": "..."}
    — décision définitive du 2026-08-02 (après un aller-retour sur "1 app partagée pour toute
    la plateforme", abandonné : le parcours de création d'app Powens n'est en réalité pas plus
    dur pour Stéphane la 2e fois, la vraie demande est un meilleur accompagnement dans l'UI, pas
    une app centrale qui retirerait au client la maîtrise de sa relation avec Powens) : chaque
    org a SA PROPRE app Powens, donc `client_id` (public, nécessaire pour la webview) est
    stocké par org. Le `auth_token` est le token PERMANENT capturé une seule fois (voir
    connector_powens.py), jamais régénéré via `/auth/init`."""
    raw = _org_secret_value(org_id, 'powens_credentials')
    if not raw:
        return None
    return json.loads(raw)


@app.route('/api/executor/powens/bootstrap', methods=['POST'])
def powens_bootstrap():
    """Crée un utilisateur Powens PERMANENT isolé pour une org, une seule fois (`POST
    /auth/init` avec client_id+client_secret de l'app Powens DE L'ORG — jamais rappelé ensuite,
    voir `_powens_credentials`). Nécessaire pour que chaque org self-service son propre
    connector Powens (retour de Stéphane, 2026-07-29 : "tous les connectors doivent être
    paramétrables" depuis le panneau d'organisation, même principe déjà en place pour Enable
    Banking).
    Ne stocke rien ici : renvoie `authToken`+`clientId` au client (Apps Script), qui les persiste
    via `identitySetOrgSecret('powens_credentials', {domain, auth_token, client_id})` — client_id
    doit être reconservé (pas juste le temps de cet appel) car `powens_start_auth` en a besoin
    à chaque webview, chaque org ayant sa propre app. Même chemin de persistance que les
    identifiants Enable Banking self-service, jamais dupliqué côté Python. `client_secret` lui
    n'est utilisé que pour cet appel, jamais journalisé ni renvoyé.
    Body: {orgId, domain, clientId, clientSecret}."""
    org_id = ''
    try:
        data = request.get_json() or {}
        org_id = data.get('orgId', '')
        domain = (data.get('domain') or '').strip()
        client_id = (data.get('clientId') or '').strip()
        client_secret = (data.get('clientSecret') or '').strip()
        if not org_id or not domain or not client_id or not client_secret:
            return jsonify({'success': False, 'error': 'orgId, domain, clientId, clientSecret requis'}), 400

        base = f"https://{domain}.biapi.pro/2.0"
        r = requests.post(f'{base}/auth/init', json={'client_id': client_id, 'client_secret': client_secret}, timeout=15)
        r.raise_for_status()
        auth_token = r.json().get('auth_token')
        if not auth_token:
            return jsonify({'success': False, 'error': "Powens n'a pas renvoyé de jeton — vérifie le domaine/les identifiants."}), 502

        return jsonify({'success': True, 'authToken': auth_token, 'clientId': client_id})

    except requests.RequestException as e:
        return _safe_upstream_error(org_id, 'powens_bootstrap',
                                     'Impossible de contacter Powens pour le moment — vérifie le domaine et les identifiants.', e)
    except Exception as e:
        return _safe_upstream_error(org_id, 'powens_bootstrap', 'Une erreur est survenue. Réessaie plus tard.', e)


@app.route('/api/executor/powens/start-auth', methods=['POST'])
def powens_start_auth():
    """Démarre une liaison de compte Powens (webview) avec l'utilisateur PERMANENT déjà associé
    au token de cette org (jamais /auth/init, voir docstring ci-dessus). Body: {orgId}."""
    org_id = ''
    try:
        data = request.get_json() or {}
        org_id = data.get('orgId', '')
        if not org_id:
            return jsonify({'success': False, 'error': 'orgId manquant'}), 400

        credentials = _powens_credentials(org_id)
        if not credentials:
            return jsonify({'success': False, 'error': 'Aucun identifiant Powens configuré pour cette organisation'}), 404

        # '18100230' = repli legacy pour smcspl uniquement, dont le secret a été enregistré
        # avant que client_id soit stocké par org (voir _powens_credentials) — toute org
        # bootstrappée après le 2026-08-02 a son propre client_id, jamais cette valeur.
        client_id = credentials.get('client_id') or '18100230'

        base = f"https://{credentials['domain']}.biapi.pro/2.0"
        headers = {'Authorization': f"Bearer {credentials['auth_token']}"}
        r = requests.get(f'{base}/auth/token/code', headers=headers, timeout=15)
        r.raise_for_status()
        temp_code = r.json()['code']

        webview_url = (
            'https://webview.powens.com/connect'
            f"?domain={credentials['domain']}"
            f'&client_id={client_id}'
            f'&redirect_uri={quote(POWENS_REDIRECT_URI, safe="")}'
            f'&code={quote(temp_code, safe="")}'
        )
        return jsonify({'success': True, 'url': webview_url})

    except requests.RequestException as e:
        return _safe_upstream_error(org_id, 'powens_start_auth',
                                     'Impossible de contacter Powens pour le moment. Réessaie ou choisis une autre banque.', e)
    except Exception as e:
        return _safe_upstream_error(org_id, 'powens_start_auth', 'Une erreur est survenue. Réessaie plus tard.', e)


def _powens_already_linked_ids(org_id):
    """IDs de comptes Powens déjà attachés à une brique Compte de cette org — pour ne jamais
    proposer/auto-attacher un compte déjà automatisé ailleurs (bug réel trouvé 2026-08-06,
    retour de Stéphane : cherchait à ajouter un compte épargne BCP, le système a auto-attaché
    silencieusement le compte COURANT déjà automatisé, sans jamais vérifier qu'il l'était déjà —
    ni la nature ni le statut "déjà lié" n'étaient contrôlés avant d'attacher)."""
    try:
        r = requests.get(f'{ANALYZOR_URL}/api/org/{org_id}/bricks', params={'type': 'Compte'}, timeout=15)
        r.raise_for_status()
        ids = set()
        for b in r.json().get('bricks', []):
            pid = (b.get('contenu') or {}).get('powens_account_id')
            if pid is not None:
                ids.add(pid)
        return ids
    except Exception:
        return set()


@app.route('/api/executor/powens/accounts', methods=['GET'])
def powens_accounts_all():
    """Tous les comptes Powens déjà accessibles pour cette org, TOUTES connexions confondues
    (2026-08-06) — permet de proposer un compte à automatiser sans relancer la webview/re-login
    si l'org a déjà une connexion vers cette banque (retour de Stéphane : "pour rechercher
    d'autres comptes il demande de rerentrer les identifiants bcp alors qu'il les a déjà depuis
    la première recherche" — `/users/me/accounts` sans filtre de connexion renvoie déjà TOUT,
    inutile de repasser par une nouvelle webview). Chaque compte porte `alreadyLinked` (déjà
    attaché à une brique Compte de cette org, voir _powens_already_linked_ids) — l'appelant ne
    doit jamais proposer/auto-attacher un compte déjà `alreadyLinked`.
    Query: orgId, bankName? (filtre optionnel, insensible à la casse, sur le nom de banque déjà
    résolu — utilisé pour ne montrer que les comptes de LA banque que l'utilisateur vient de
    choisir dans la recherche, pas tous ses comptes Powens toutes banques confondues)."""
    org_id = request.args.get('orgId', '')
    try:
        bank_name_filter = (request.args.get('bankName') or '').strip().lower()
        if not org_id:
            return jsonify({'success': False, 'error': 'orgId manquant'}), 400
        credentials = _powens_credentials(org_id)
        if not credentials:
            return jsonify({'success': False, 'error': 'Aucun identifiant Powens configuré pour cette organisation'}), 404

        base = f"https://{credentials['domain']}.biapi.pro/2.0"
        headers = {'Authorization': f"Bearer {credentials['auth_token']}"}

        r = requests.get(f'{base}/users/me/connections', headers=headers, timeout=15)
        r.raise_for_status()
        connections = {c['id']: c.get('id_bank') for c in r.json().get('connections', [])}

        banks_raw = _powens_banks_raw(org_id)
        bank_name_by_id = {b['id']: b['name'] for b in banks_raw}

        r2 = requests.get(f'{base}/users/me/accounts', headers=headers, timeout=15)
        r2.raise_for_status()
        accounts = r2.json().get('accounts', [])

        already_linked = _powens_already_linked_ids(org_id)
        out = []
        for a in accounts:
            id_bank = connections.get(a.get('id_connection'))
            bank_name = bank_name_by_id.get(id_bank)
            if bank_name_filter and (bank_name or '').strip().lower() != bank_name_filter:
                continue
            a['bankName'] = bank_name
            a['alreadyLinked'] = a.get('id') in already_linked
            out.append(a)

        return jsonify({'success': True, 'accounts': out})

    except requests.RequestException as e:
        return _safe_upstream_error(org_id, 'powens_accounts_all',
                                     'Impossible de récupérer tes comptes pour le moment. Réessaie dans un instant.', e)
    except Exception as e:
        return _safe_upstream_error(org_id, 'powens_accounts_all', 'Une erreur est survenue. Réessaie plus tard.', e)


@app.route('/api/executor/powens/link-connection', methods=['POST'])
def powens_link_connection():
    """Après la connexion bancaire (webview Powens redirigée vers https://structory.ai/
    ?connection_id=...), l'utilisateur colle ce lien (ou juste le connection_id, ou il arrive
    automatiquement via postMessage) pour récupérer la liste des comptes de CETTE connexion
    précise. Body: {orgId, connectionIdOrUrl}.

    Bug réel trouvé et corrigé le 2026-07-28 (retour de Stéphane : "ça m'a fait apparaître
    tous les BCP") : `/users/me/accounts` renvoie TOUS les comptes de TOUTES les connexions
    Powens de l'org, pas seulement ceux de la connexion qu'on vient d'établir — filtré
    maintenant sur `id_connection` (via `/users/me/connections`). Chaque compte porte aussi
    `bankName` (résolu via `id_bank` de la connexion + catalogue `/2.0/banks`) — jamais
    "Powens" comme nom de banque (2e bug trouvé : Stéphane voyait "Powens" pré-rempli comme
    établissement en créant la fiche, exactement le genre de confusion agrégateur/banque que
    la recherche par nom devait éliminer)."""
    org_id = ''
    try:
        data = request.get_json() or {}
        org_id = data.get('orgId', '')
        raw = (data.get('connectionIdOrUrl') or '').strip()
        if not org_id or not raw:
            return jsonify({'success': False, 'error': 'orgId et connectionIdOrUrl requis'}), 400

        m = re.search(r'connection_id=(\d+)', raw)
        connection_id = int(m.group(1)) if m else int(raw)

        credentials = _powens_credentials(org_id)
        if not credentials:
            return jsonify({'success': False, 'error': 'Aucun identifiant Powens configuré pour cette organisation'}), 404

        base = f"https://{credentials['domain']}.biapi.pro/2.0"
        headers = {'Authorization': f"Bearer {credentials['auth_token']}"}

        r = requests.get(f'{base}/users/me/connections', headers=headers, timeout=15)
        r.raise_for_status()
        connections = {c['id']: c.get('id_bank') for c in r.json().get('connections', [])}
        id_bank = connections.get(connection_id)

        banks_raw = _powens_banks_raw(org_id)
        bank_name = next((b['name'] for b in banks_raw if b['id'] == id_bank), None)

        r2 = requests.get(f'{base}/users/me/accounts', headers=headers, timeout=15)
        r2.raise_for_status()
        accounts = [a for a in r2.json().get('accounts', []) if a.get('id_connection') == connection_id]
        # `alreadyLinked` (2026-08-06, voir _powens_already_linked_ids) : bug réel corrigé —
        # l'appelant (Navigator) auto-attachait le seul compte renvoyé SANS jamais vérifier s'il
        # était déjà attaché à une autre brique Compte (cas réel : recherche d'un compte épargne
        # BCP, le compte COURANT déjà automatisé était le seul renvoyé et se faisait ré-attacher
        # silencieusement, comme si un nouveau compte avait été trouvé).
        already_linked = _powens_already_linked_ids(org_id)
        for a in accounts:
            a['bankName'] = bank_name
            a['alreadyLinked'] = a.get('id') in already_linked

        return jsonify({'success': True, 'connectionId': connection_id, 'accounts': accounts})

    except requests.RequestException as e:
        return _safe_upstream_error(org_id, 'powens_link_connection',
                                     'Impossible de récupérer tes comptes pour le moment. Réessaie dans un instant.', e)
    except (ValueError, TypeError):
        return jsonify({'success': False, 'error': 'Lien ou numéro de connexion invalide'}), 400
    except Exception as e:
        return _safe_upstream_error(org_id, 'powens_link_connection', 'Une erreur est survenue. Réessaie plus tard.', e)


# ============================================================
# RECHERCHE DE BANQUE (2026-07-28, retour de Stéphane après le test Powens/Enable Banking :
# "une personne ne connaît pas et se fout de enablebanking, powens ou autre c'est du chinois...
# lorsqu'on veut ajouter un compte on cherche le nom de la banque"). Fusionne les catalogues
# Powens (`/2.0/banks`, les banques RÉELLEMENT accessibles à l'app de cette org) et Enable
# Banking (`/aspsps`, ~800 ASPSPs) — jamais le nom d'un connector exposé à l'utilisateur final,
# juste sa banque. Cache en mémoire (24h, un catalogue change rarement) pour ne pas rappeler ces
# APIs à chaque frappe clavier.
# ============================================================
_BANK_DIRECTORY_TTL_SECONDS = 24 * 60 * 60
_bank_directory_cache = {}

# Domaine Powens partagé (l'app "smc"), utilisé comme catalogue PAR DÉFAUT pour toute org qui
# n'a pas encore configuré son propre domaine Powens (2026-08-01, retour de Stéphane : "smcdemo
# c'est pas possible qu'il trouve aucune banque"). Vérifié en conditions réelles : GET
# /2.0/connectors/ répond SANS AUCUNE authentification (curl direct, code 200, catalogue complet
# ~1875 banques) — ce n'est PAS un raccourci qui expose un compte/utilisateur Powens d'une autre
# org, c'est un catalogue public par construction de l'API. Ne sert QUE pour PARCOURIR les
# banques disponibles ; la LIAISON réelle d'un compte reste strictement gated par les
# identifiants PROPRES de l'org (_powens_credentials, jamais contourné ailleurs).
POWENS_DEFAULT_CATALOG_DOMAIN = 'smc-sandbox'


def _powens_banks_raw(org_id):
    """Catalogue Powens brut ({id, name, hidden, ...}), cache 24h — utilisé pour la recherche
    (_powens_bank_list) ET pour résoudre le nom de banque d'une connexion (powens_link_connection,
    via id_bank). Factorisé pour ne jamais avoir deux caches/deux appels API différents pour la
    même donnée. Utilise le domaine PROPRE de l'org si configuré, sinon le domaine partagé par
    défaut (catalogue public, voir POWENS_DEFAULT_CATALOG_DOMAIN) — jamais bloquant pour la
    simple consultation du catalogue, contrairement à la liaison réelle d'un compte."""
    credentials = _powens_credentials(org_id)
    domain = credentials['domain'] if credentials else POWENS_DEFAULT_CATALOG_DOMAIN
    cache_key = 'powens_raw:' + domain
    cached = _bank_directory_cache.get(cache_key)
    if cached and time.time() - cached['ts'] < _BANK_DIRECTORY_TTL_SECONDS:
        return cached['banks']

    base = f"https://{domain}.biapi.pro/2.0"
    # Catalogue PUBLIC (vérifié : /2.0/connectors/ répond sans en-tête d'authentification) —
    # jamais besoin d'un auth_token pour PARCOURIR les banques disponibles, seulement pour en
    # LIER une réellement (start-auth, toujours gated par les identifiants propres de l'org).
    r = requests.get(f'{base}/connectors/', timeout=15)
    r.raise_for_status()
    banks = r.json().get('connectors', [])
    _bank_directory_cache[cache_key] = {'banks': banks, 'ts': time.time()}
    return banks


def _powens_bank_list(org_id):
    banks = _powens_banks_raw(org_id)
    return [{'name': b['name'], 'connector': 'powens'} for b in banks if not b.get('hidden')]


def _enablebanking_bank_list(org_id):
    credentials, mode = _eb_credentials(org_id)
    # Mode sandbox : /auth force Mock ASPSP quoi qu'on envoie (voir enablebanking_start_auth) —
    # montrer les ~800 vraies banques du catalogue dans la recherche alors qu'aucune n'est
    # réellement connectable serait trompeur, exclu de la recherche. Mode 'restricted' (2026-07-29,
    # retour de Stéphane : chercher "Crédit Mutuel" ne trouvait rien alors que l'org a DÉJÀ un
    # vrai accès Enable Banking pour ce compte) INCLUS ici : /aspsps donne le catalogue complet
    # (2632 banques, vérifié) même avec une app Restricted — seule la LIAISON peut échouer, pas
    # la lecture du catalogue. Ne jamais cacher une banque réellement reconnue par Enable
    # Banking juste parce qu'on ne sait pas encore si la liaison va réussir.
    if not credentials or mode not in ('production', 'restricted'):
        return []
    cache_key = 'eb:' + credentials['app_id']
    cached = _bank_directory_cache.get(cache_key)
    if cached and time.time() - cached['ts'] < _BANK_DIRECTORY_TTL_SECONDS:
        return cached['banks']

    token = connector_enablebanking.sign_jwt(credentials)
    r = requests.get(f'{connector_enablebanking.EB_API_BASE}/aspsps', headers={'Authorization': f'Bearer {token}'}, timeout=15)
    r.raise_for_status()
    # "Mock ASPSP <pays>" n'est jamais une vraie banque — jamais montré à l'utilisateur final,
    # que l'org soit en mode sandbox ou production (le mode sandbox reste utilisable via le
    # bouton Enable Banking existant, pas besoin de l'exposer dans la recherche).
    banks = [
        {'name': a['name'], 'connector': 'enablebanking', 'country': a.get('country'),
         'aspspName': a['name'], 'aspspCountry': a.get('country'), 'ebMode': mode,
         'logo': a.get('logo')}  # Enable Banking fournit un vrai logo par ASPSP (2026-08-01,
                                  # retour de Stéphane : "les logos des banques devraient
                                  # apparaître") — Powens (voir _powens_bank_list) n'expose
                                  # aucun champ logo documenté, jamais inventé côté nous.
        for a in r.json().get('aspsps', [])
        if not a['name'].startswith('Mock ASPSP')
    ]
    _bank_directory_cache[cache_key] = {'banks': banks, 'ts': time.time()}
    return banks


@app.route('/api/executor/connector-flow', methods=['GET'])
def connector_flow():
    """Premier point de sortie du modèle de domaine (voir core/entity.py, core/connector.py,
    core/flow.py — décision de Stéphane, 2026-08-01 : "le modèle canonique de Structory", une
    API externe ne doit jamais traverser directement une vue). Renvoie un objet Flow
    (core.flow.Flow.to_dict()) représentant l'état du Connector d'un compte déjà automatisé —
    PrecognFlow (Navigator) en est UN rendu, une future vue conversationnelle/vocale pourrait en
    être un autre, sans jamais recalculer cette logique indépendamment.
    Query: ?orgId=...&etablissement=...&nature=...&module=..."""
    org_id = request.args.get('orgId', '')
    etablissement = request.args.get('etablissement', '')
    nature = request.args.get('nature', '')
    module = request.args.get('module')
    if not org_id or not etablissement or not nature:
        return jsonify({'success': False, 'error': 'orgId, etablissement, nature requis'}), 400

    try:
        r = requests.get(
            f'{ANALYZOR_URL}/api/connectors/resolve',
            params={'etablissement': etablissement, 'nature': nature, 'orgId': org_id, 'module': module},
            timeout=30,
        )
        r.raise_for_status()
        matches = r.json().get('connectors', [])
    except requests.RequestException as e:
        return _safe_upstream_error(org_id, 'connector_flow', 'Caractéristiques du connector indisponibles pour le moment.', e)

    if not matches:
        connector = Connector.unconfigured(etablissement, nature)
        flow = Flow.bank_connection(connecteur_label=None)
        # Aucun nœud n'est "success" ici : compte sans connector résolu (saisie manuelle),
        # jamais présenté comme une connexion établie.
    else:
        match = matches[0]
        connector = Connector(interface=match['interface'], etablissement=etablissement, nature=nature,
                               status=STATUS_ACTIVE, brick_id=match.get('brickId'))
        flow = Flow.bank_connection_completed(etablissement, connector)

    return jsonify({'success': True, 'connector': connector.to_dict(), 'flow': flow.to_dict()})


@app.route('/api/executor/banks/search', methods=['GET'])
def banks_search():
    """Recherche de banque par nom, tous connectors confondus — jamais un nom de connector
    exposé au résultat de recherche. Query: ?orgId=...&q=...

    Dédoublonnage (2026-07-29) : depuis que le catalogue Enable Banking "restricted" est inclus
    (voir _eb_credentials), une même banque peut apparaître via Powens ET Enable Banking (ex.
    "Crédit Mutuel") — jamais deux lignes identiques pour la même banque.

    Priorité (révisée 2026-07-29 soir, après confirmation réelle) : Enable Banking en mode
    'production' (vrais identifiants self-service, peut réellement lier) passe devant Powens —
    mais en mode 'restricted' (app existante, restreinte à la synchro des comptes déjà liés),
    Stéphane a testé et confirmé que la LIAISON d'un nouveau compte échoue vraiment ("Impossible
    de contacter cette banque"). Proposer en premier une option qu'on SAIT ne pas fonctionner
    n'aide personne, même si Enable Banking reste la priorité produit globale — Powens (prouvé
    fonctionnel, voir BCP) passe donc devant pour tout doublon en mode 'restricted'."""
    org_id = request.args.get('orgId', '')
    query = (request.args.get('q') or '').strip().lower()
    # `includeAll=1` (2026-07-31, voir le Flow visible côté Navigator) : ne déduplique PAS —
    # utilisé uniquement par la reprise d'erreur ("essayer l'autre connecteur") pour retrouver
    # une entrée EnableBanking/Powens alternative pour le MÊME nom de banque, alors que la
    # recherche normale masque volontairement ce doublon.
    include_all = request.args.get('includeAll') == '1'
    if not org_id:
        return jsonify({'success': False, 'error': 'orgId manquant'}), 400
    try:
        eb_banks = _enablebanking_bank_list(org_id)
        powens_banks = _powens_bank_list(org_id)
    except requests.RequestException as e:
        return _safe_upstream_error(org_id, 'banks_search', 'Recherche indisponible pour le moment. Réessaie dans un instant.', e)

    if include_all:
        banks = eb_banks + powens_banks
        if query:
            folded_query = _fold_accents(query)
            banks = [b for b in banks if folded_query in _fold_accents(b['name'].lower())]
        banks.sort(key=lambda b: b['name'])
        return jsonify({'success': True, 'banks': banks[:30]})

    eb_production = [b for b in eb_banks if b.get('ebMode') == 'production']
    eb_restricted = [b for b in eb_banks if b.get('ebMode') == 'restricted']

    seen_names = {b['name'].strip().lower() for b in eb_production}
    powens_first = [b for b in powens_banks if b['name'].strip().lower() not in seen_names]
    seen_names |= {b['name'].strip().lower() for b in powens_first}
    banks = eb_production + powens_first + [b for b in eb_restricted if b['name'].strip().lower() not in seen_names]

    if query:
        # Insensible aux accents (bug réel trouvé 2026-07-29, retour de Stéphane : chercher
        # "credit mutuel" sans accent ne trouvait rien alors que "Crédit Mutuel" existe bien
        # dans le catalogue) — comparaison sur les noms "repliés", jamais sur l'affichage.
        folded_query = _fold_accents(query)
        banks = [b for b in banks if folded_query in _fold_accents(b['name'].lower())]
    banks.sort(key=lambda b: b['name'])
    return jsonify({'success': True, 'banks': banks[:30]})


def _fold_accents(s):
    """Replie les accents pour une comparaison insensible ("credit" doit trouver "Crédit")."""
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))


def _patrimoine_view_data(org_id, module):
    """Comptes d'une org + solde/dernière écriture (ledger_api, un seul appel batch) + mode de
    synchro par compte (résolution connector, Analyzor). Factorisé (2026-07-26) : utilisé par la
    route `/api/executor/patrimoine-view` (Navigator) ET par `daily_report` (email quotidien) —
    les deux ont besoin exactement des mêmes faits, jamais dupliqués. Lève `requests.RequestException`
    telle quelle, à l'appelant de la traduire en réponse HTTP appropriée.

    timeout=20 (pas 10) : le scan Drive à froid pour la liste des comptes (mesuré ~9s pour 18
    briques, 2026-07-26) est maintenant mis en cache côté Analyzor (bricks.py::list_bricks,
    6h de TTL) mais le tout premier appel après un redémarrage reste proche de l'ancienne
    limite de 10s — cause réelle d'un 502 intermittent observé en usage réel avant ce correctif."""
    r = requests.get(f'{ANALYZOR_URL}/api/org/{org_id}/bricks', params={'type': 'Compte'}, timeout=30)
    r.raise_for_status()
    comptes_bricks = r.json().get('bricks', [])

    comptes_payload = []
    for b in comptes_bricks:
        c = b.get('contenu', {})
        comptes_payload.append({
            'etablissement': c.get('etablissement'), 'nature': c.get('nature'),
            'titulaire': c.get('titulaire'), 'produit': c.get('produit'), 'devise': c.get('devise_origine') or 'EUR',
        })

    soldes_by_key = {}
    if comptes_payload:
        r = requests.post(f'{LEDGER_API_URL}/api/ledger/comptes-solde', json={'orgId': org_id, 'comptes': comptes_payload}, timeout=15)
        r.raise_for_status()
        for item in r.json().get('comptes', []):
            soldes_by_key[(item.get('etablissement'), item.get('nature'), item.get('titulaire'), item.get('produit'))] = item

    # Résolution BATCH des connectors (2026-08-02, retour de Stéphane : "le chargement patrimoine
    # est toujours trop long... on dirait que c'est planté") — root cause réelle : un appel HTTP
    # séparé par compte (jusqu'à 19-20) juste pour connaître le syncMode, alors que ces briques
    # Rule sont déjà en cache côté Analyzor par dossier, pas par établissement+nature. Un seul
    # aller-retour réseau pour TOUS les comptes désormais (voir /api/connectors/resolve-batch).
    sync_modes = ['manual'] * len(comptes_bricks)
    try:
        rc = requests.post(
            f'{ANALYZOR_URL}/api/connectors/resolve-batch',
            json={'orgId': org_id, 'module': module, 'comptes': comptes_payload},
            timeout=30,
        )
        rc.raise_for_status()
        batch_results = rc.json().get('results', [])
        sync_modes = ['api' if matches else 'manual' for matches in batch_results]
    except requests.RequestException:
        pass  # résolution indisponible : tout reste 'manual', jamais bloquant pour l'affichage

    comptes = []
    for b, sync_mode in zip(comptes_bricks, sync_modes):
        c = b.get('contenu', {})
        etablissement = c.get('etablissement')
        nature = c.get('nature')
        titulaire = c.get('titulaire')
        produit = c.get('produit')

        solde_info = soldes_by_key.get((etablissement, nature, titulaire, produit), {})
        comptes.append({
            'uid': b.get('uid'),
            'nom': c.get('nom') or b.get('title'),
            'numero': c.get('numero'),
            'iban': c.get('iban'),
            'etablissement': etablissement,
            'nature': nature,
            'titulaire': titulaire,
            'produit': produit,
            'devise': c.get('devise_origine') or 'EUR',
            'solde': solde_info.get('solde', 0.0),
            'lastDate': solde_info.get('lastDate'),
            'syncMode': sync_mode,
        })

    return comptes


@app.route('/api/executor/patrimoine-view', methods=['GET'])
def patrimoine_view():
    """Vue agrégée pour une page "application bancaire" (Navigator, 2026-07-26, retour de
    Stéphane : "je dois comprendre en moins de 3 secondes combien j'ai, où, comment"). Read-only,
    ne poste jamais rien — jamais confondu avec `/api/executor/sync`.

    Query: orgId, module?
    """
    try:
        org_id = request.args.get('orgId', '')
        module = request.args.get('module')
        if not org_id:
            return jsonify({'success': False, 'error': 'orgId manquant'}), 400

        try:
            comptes = _patrimoine_view_data(org_id, module)
        except requests.RequestException as e:
            return jsonify({'success': False, 'error': f'service injoignable : {e}'}), 502

        # Total consolidé en EUR (retour de Stéphane, 2026-07-26 : "un vrai état du compte en
        # euros qui cumule les euros et les US$ convertis" — le montant "dont X USD" affiché
        # séparément sans conversion donnait l'impression trompeuse d'un mélange). Même
        # mécanisme que le total déjà converti de l'email quotidien (_fx_to_eur, taux BCE
        # via frankfurter.app) — jamais utilisé côté ledger_api/comptabilité elle-même, affichage
        # uniquement, voir _fx_to_eur.
        totals_by_devise = {}
        for c in comptes:
            totals_by_devise[c['devise']] = totals_by_devise.get(c['devise'], 0.0) + (c['solde'] or 0.0)
        total_eur = sum(v * _fx_to_eur(d) for d, v in totals_by_devise.items())

        return jsonify({'success': True, 'comptes': comptes, 'totalsByDevise': totals_by_devise, 'totalEur': round(total_eur, 2)})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/executor/time-points', methods=['GET'])
def time_points():
    """Relais vers `ledger_api::/api/ledger/time-points` (brique Time, 2026-08-03) — Navigator
    ne parle jamais directement à ledger_api, toujours via l'Executor, même principe que le
    reste de cette API. Query: orgId."""
    org_id = request.args.get('orgId', '')
    try:
        if not org_id:
            return jsonify({'success': False, 'error': 'orgId manquant'}), 400
        r = requests.get(f'{LEDGER_API_URL}/api/ledger/time-points', params={'orgId': org_id}, timeout=15)
        r.raise_for_status()
        return jsonify(r.json())
    except requests.RequestException as e:
        return _safe_upstream_error(org_id, 'time_points', 'Impossible de récupérer l’historique pour le moment.', e)


@app.route('/api/executor/patrimoine-at', methods=['GET'])
def patrimoine_at():
    """Patrimoine consolidé à une date passée (brique Time, 2026-08-03) + comparaison avec
    aujourd'hui — même principe de calcul que `_build_patrimoine_payload` (somme des vraies
    briques Compte, jamais le grand livre brut) mais paramétré par une date choisie plutôt que
    "hier" uniquement. `date` doit être une des dates renvoyées par `/api/executor/time-points`
    (un vrai constat de solde), pas une date arbitraire — sinon la comparaison n'a pas de sens
    ("Time" = un point où l'état était réellement connu, pas un jour quelconque).

    Query: orgId, module?, date (YYYY-MM-DD ou YYYY/MM/DD)
    """
    org_id = request.args.get('orgId', '')
    try:
        module = request.args.get('module')
        date = (request.args.get('date') or '').replace('-', '/')
        if not org_id or not date:
            return jsonify({'success': False, 'error': 'orgId et date requis'}), 400

        try:
            comptes = _patrimoine_view_data(org_id, module)
        except requests.RequestException as e:
            return jsonify({'success': False, 'error': f'service injoignable : {e}'}), 502

        # `_comptes_soldes_at` est exclusif (solde à la FIN du jour précédent `end_date`, voir sa
        # docstring) — pour le solde À la date choisie incluse, il faut viser le lendemain.
        end_date_exclusive = (datetime.strptime(date, '%Y/%m/%d') + timedelta(days=1)).strftime('%Y/%m/%d')
        soldes_at_date = _comptes_soldes_at(org_id, comptes, end_date_exclusive)

        comptes_out = []
        totals_now = {}
        totals_then = {}
        for c in comptes:
            solde_then = soldes_at_date.get((c['etablissement'], c['nature'], c['titulaire'], c.get('produit')))
            solde_then = solde_then if solde_then is not None else 0.0
            devise = c['devise']
            totals_now[devise] = totals_now.get(devise, 0.0) + c['solde']
            totals_then[devise] = totals_then.get(devise, 0.0) + solde_then
            comptes_out.append({
                'nom': c['nom'], 'numero': c.get('numero'), 'devise': devise,
                'soldeNow': c['solde'], 'soldeAtDate': solde_then,
                'delta': round(c['solde'] - solde_then, 2),
            })
        comptes_out.sort(key=lambda c: (c['numero'] is None, c['numero'] if c['numero'] is not None else 0))

        fx_rates = {d: _fx_to_eur(d) for d in set(totals_now) | set(totals_then)}
        total_now_eur = round(sum(totals_now.get(d, 0.0) * fx_rates[d] for d in fx_rates), 2)
        total_then_eur = round(sum(totals_then.get(d, 0.0) * fx_rates[d] for d in fx_rates), 2)

        return jsonify({
            'success': True,
            'date': date,
            'comptes': comptes_out,
            'totalEurNow': total_now_eur,
            'totalEurAtDate': total_then_eur,
            'deltaEur': round(total_now_eur - total_then_eur, 2),
        })

    except Exception as e:
        return _safe_upstream_error(org_id, 'patrimoine_at', 'Une erreur est survenue. Réessaie plus tard.', e)


@app.route('/api/executor/balance-point', methods=['POST'])
def manual_balance_point():
    """Constat de solde déclenché par un humain (Communicator), pas par un cycle de sync
    automatique — mais passe par le même gateway que /api/executor/sync vers ledger_api,
    jamais un appel direct ledger_api depuis un outil consommateur. Paramétré par org comme
    tout le reste de l'Executor (2026-07-21, retour de Stéphane : "communicator = j'entre une
    instruction = après j'envoie à l'executor qui doit être paramétré selon l'organisation,
    pour tous partout").

    Body: {orgId, etablissement, nature, titulaire?, produit?, solde, devise?, date?}
    """
    try:
        data = request.get_json() or {}
        org_id = data.get('orgId', '')
        etablissement = data.get('etablissement', '')
        nature = data.get('nature', '')
        if not org_id or not etablissement or not nature or data.get('solde') is None:
            return jsonify({'success': False, 'error': 'orgId, etablissement, nature, solde requis'}), 400

        try:
            r = requests.post(f'{LEDGER_API_URL}/api/ledger/balance-point', json={
                'orgId': org_id,
                'etablissement': etablissement,
                'nature': nature,
                'titulaire': data.get('titulaire'),
                'produit': data.get('produit'),
                'solde': data['solde'],
                'devise': data.get('devise'),
                'date': data.get('date'),
            }, timeout=10)
            r.raise_for_status()
            result = r.json()
        except requests.RequestException as e:
            return jsonify({'success': False, 'error': f'ledger_api injoignable : {e}'}), 502

        log_to_journal(
            org_id, 'executor',
            f"Point de solde manuel : {result.get('compte')} = {data['solde']} (écart {result.get('ecart')})",
            ['Déclenché depuis Communicator'],
        )

        return jsonify(result)

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


def _org_smtp_secret(org_id):
    """Config SMTP propre à cette org (saisie via Communicator, "configure email host=...
    port=... user=... password=..."), chiffrée au repos (org_secrets.py, Fernet) — lue via la
    route strictement interne d'Analyzor (même clé partagée que subscriptions_api), jamais un
    identifiant en dur partagé entre orgs (2026-07-22, retour de Stéphane : "il faut une
    solution pour paramétrer son serveur depuis Suivre Mes Comptes", "pour tous partout")."""
    r = requests.get(
        f'{ANALYZOR_URL}/api/org/{org_id}/secrets/email_smtp/value',
        # 30s (pas 10) : depuis l'ajout de la synchro avant le calcul du patrimoine
        # (2026-07-27), Analyzor peut être sollicité juste avant cet appel (résolution
        # connector par compte, ~24s à froid) — un timeout de 10s a fait échouer l'envoi de
        # l'email du 27/07 alors qu'Analyzor répondait, juste plus lentement. Job en tâche de
        # fond une fois par jour, quelques secondes de plus ne coûtent rien ici.
        headers={'X-Service-Key': SUBSCRIPTIONS_SERVICE_KEY}, timeout=30,
    )
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return json.loads(r.json()['value'])


def _send_email_smtp(smtp_config, to, subject, text_body, html_body):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = smtp_config['user']
    msg['To'] = to
    # texte en premier (fallback pour clients sans HTML/filtres anti-spam), html en dernier —
    # ordre standard MIME multipart/alternative : le client choisit la DERNIÈRE partie qu'il sait
    # rendre.
    msg.attach(MIMEText(text_body, 'plain', _charset='utf-8'))
    msg.attach(MIMEText(html_body, 'html', _charset='utf-8'))

    port = int(smtp_config['port'])
    with smtplib.SMTP(smtp_config['host'], port, timeout=15) as server:
        server.starttls()
        server.login(smtp_config['user'], smtp_config['password'])
        server.sendmail(smtp_config['user'], [to], msg.as_string())


_CURRENCY_SYMBOLS = {'EUR': '€', 'USD': '$', 'GBP': '£'}

_fx_cache = {}
_FX_CACHE_TTL_SECONDS = 6 * 60 * 60  # 6h : un taux de change n'a pas besoin d'être temps réel


def _fx_to_eur(devise):
    """Taux de change devise->EUR (taux BCE, api.frankfurter.app, gratuit sans clé). Uniquement
    pour l'AFFICHAGE d'un total consolidé dans l'email quotidien (2026-07-26, retour de
    Stéphane : "faut regrouper tout en euro [et] convertir le montant cumulé en dollar") —
    jamais utilisé côté ledger_api/comptabilité, qui elle ne convertit jamais entre devises
    (§0 ARCHITECTURE.md, règle intacte : le journal reste dans les devises d'origine). 1.0 si la
    devise est déjà EUR ou si le taux est indisponible (dégradation gracieuse : mieux vaut un
    total légèrement faux marqué comme tel qu'un email qui échoue à s'envoyer)."""
    if devise == 'EUR':
        return 1.0
    cached = _fx_cache.get(devise)
    if cached and (datetime.now().timestamp() - cached['t']) < _FX_CACHE_TTL_SECONDS:
        return cached['rate']
    try:
        r = requests.get('https://api.frankfurter.app/latest', params={'from': devise, 'to': 'EUR'}, timeout=8)
        r.raise_for_status()
        rate = r.json()['rates']['EUR']
        _fx_cache[devise] = {'rate': rate, 't': datetime.now().timestamp()}
        return rate
    except Exception:
        return cached['rate'] if cached else 1.0


def _fmt_amount(v, devise):
    """Format "à la française" : espace pour les milliers, virgule pour les décimales, symbole
    collé (110 015,93 €) — jamais de conversion entre devises (§0 ARCHITECTURE.md), un montant
    reste toujours dans SA devise d'origine."""
    s = f'{v:,.2f}'.replace(',', ' ').replace('.', ',')
    symbole = _CURRENCY_SYMBOLS.get(devise)
    return f'{s} {symbole}' if symbole else f'{s} {devise}'


def _fmt_variation(v, devise):
    signe = '+' if v > 0 else ('−' if v < 0 else '')
    return f'{signe}{_fmt_amount(abs(v), devise)}'


def _build_report_email(payload):
    """payload : {date, comptes: [{nom, solde, devise, variation}], totalEur, variationEur,
    navigatorUrl, ownerName}. Retourne (subject, text_body, html_body) — rapport patrimonial
    (2026-07-26, retour de Stéphane : "Suivre Mes Comptes devrait envoyer un email qui ressemble
    davantage à un rapport patrimonial qu'à une notification automatique"), pas un dump de
    chiffres bruts.

    Refondu le 2026-07-28 (retour de Stéphane sur le 1er email réel reçu) : plus de ligne "dont
    X€ Y$ — taux" ni de section "En bref" (redondantes/confuses), plus de "N compte(s) à
    synchroniser manuellement" (jugé factuellement faux et hors-sujet dans un email — cette
    info vit sur le site), plus de "plus forte variation" isolée (sans commentaire LLM ça n'a
    pas d'intérêt, à réintroduire plus tard avec le Communicator). À la place : la vraie liste
    des comptes et leurs montants, avec la variation du jour PAR COMPTE affichée uniquement
    quand elle est non nulle — jamais un chiffre à zéro qui n'apporte rien à lire.

    Total consolidé en EUR (2026-07-26, retour de Stéphane : "faut regrouper tout en euro,
    convertir le montant cumulé en dollar") — UNIQUEMENT une conversion d'affichage pour cet
    email (`totalEur`/`variationEur` déjà calculés par l'appelant via `_fx_to_eur`), le journal
    ledger-cli lui-même ne convertit jamais entre devises (§0 ARCHITECTURE.md, règle intacte).
    Chaque compte de la liste reste affiché dans SA devise d'origine, jamais converti."""
    prenom = payload.get('ownerName')
    salutation = f'Bonjour {prenom},' if prenom else 'Bonjour,'
    subject = f"💰 Suivre Mes Comptes — {payload['date']}"
    comptes = payload.get('comptes', [])

    # --- texte brut (fallback) ---
    text_lines = [salutation, '', f"Voici la situation de tes comptes au {payload['date']}.", '']
    text_lines.append(f"{_fmt_amount(payload['totalEur'], 'EUR')} ({_fmt_variation(payload['variationEur'], 'EUR')} depuis hier)")
    text_lines.append('')
    text_lines.append('Comptes :')
    for c in comptes:
        ligne = f"  · {c['nom']} : {_fmt_amount(c['solde'], c['devise'])}"
        if c.get('variation'):
            ligne += f" ({_fmt_variation(c['variation'], c['devise'])} depuis hier)"
        text_lines.append(ligne)
    text_lines += ['', f"Ouvrir Suivre Mes Comptes : {payload['navigatorUrl']}"]
    text_body = '\n'.join(text_lines)

    # --- HTML (rendu principal) ---
    variation_color = '#1a8a4a' if payload['variationEur'] > 0 else ('#c0392b' if payload['variationEur'] < 0 else '#6b7280')
    hero_html = f'''
      <div style="font-size:36px;font-weight:700;color:#0B0F10;line-height:1.2;">{_fmt_amount(payload['totalEur'], 'EUR')}</div>
      <div style="font-size:14px;color:{variation_color};margin-bottom:14px;">
        {_fmt_variation(payload['variationEur'], 'EUR')} depuis hier
      </div>'''

    def _compte_row_html(c):
        variation_html = ''
        if c.get('variation'):
            couleur = '#1a8a4a' if c['variation'] > 0 else '#c0392b'
            variation_html = f'<div style="font-size:11px;color:{couleur};">{_fmt_variation(c["variation"], c["devise"])} depuis hier</div>'
        return f'''
      <tr>
        <td style="padding:7px 0;font-size:13px;color:#374151;border-top:1px solid #f3f4f6;">{c['nom']}</td>
        <td style="padding:7px 0;font-size:13px;color:#374151;text-align:right;border-top:1px solid #f3f4f6;">
          {_fmt_amount(c['solde'], c['devise'])}
          {variation_html}
        </td>
      </tr>'''

    comptes_html = ''.join(_compte_row_html(c) for c in comptes)

    html_body = f'''<!DOCTYPE html>
<html><body style="margin:0;padding:0;background:#f3f4f6;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f6;padding:24px 0;">
<tr><td align="center">
<table role="presentation" width="480" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;max-width:480px;width:100%;">
  <tr><td style="padding:24px 28px 4px;">
    <div style="font-size:15px;font-weight:600;color:#374151;">💰 Suivre Mes Comptes</div>
  </td></tr>
  <tr><td style="padding:8px 28px 0;">
    <div style="font-size:14px;color:#6b7280;">{salutation}</div>
    <div style="font-size:14px;color:#6b7280;margin-bottom:16px;">Voici la situation de tes comptes au {payload['date']}.</div>
  </td></tr>
  <tr><td style="padding:0 28px;">
    {hero_html}
  </td></tr>
  <tr><td style="padding:0 28px 20px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
      {comptes_html}
    </table>
  </td></tr>
  <tr><td style="padding:0 28px 20px;" align="center">
    <a href="{payload['navigatorUrl']}" style="display:inline-block;background:#0B0F10;color:#ffffff;text-decoration:none;font-size:14px;font-weight:600;padding:12px 28px;border-radius:8px;">Ouvrir Suivre Mes Comptes →</a>
  </td></tr>
  <tr><td style="padding:16px 28px;border-top:1px solid #f3f4f6;">
    <div style="font-size:11px;color:#9ca3af;">Suivre Mes Comptes — rapport automatique quotidien</div>
  </td></tr>
</table>
</td></tr>
</table>
</body></html>'''

    return subject, text_body, html_body


def _org_owner_info(org_id):
    """Email (+ prénom si jamais renseigné un jour — pas de champ `name` dans subscriptions_api
    aujourd'hui, dégradation gracieuse sans nom plutôt qu'un pseudo-nom dérivé de l'email) du
    propriétaire d'une org. Jamais un email codé en dur pour une org précise : ce mécanisme doit
    marcher pour n'importe quelle org (2026-07-22, retour de Stéphane : "des output [...] à
    paramétrer avec des connectors dédiés [...] pour tous partout")."""
    r = requests.get(
        f'{SUBSCRIPTIONS_API_URL}/api/org/profile', params={'orgId': org_id},
        headers={'X-Service-Key': SUBSCRIPTIONS_SERVICE_KEY}, timeout=10,
    )
    r.raise_for_status()
    org = r.json().get('org') or {}
    for m in org.get('members', []):
        if m.get('role') == 'owner':
            return {'email': m.get('email'), 'name': m.get('name') or (m.get('info') or {}).get('name')}
    return None


def _comptes_soldes_at(org_id, comptes, end_date):
    """Solde de chaque compte à `end_date` (exclusif, sémantique ledger-cli standard — donne le
    solde à la fin du jour PRÉCÉDENT `end_date`) — un seul appel batch ledger_api. Factorisé
    (2026-07-27) : utilisé pour la variation quotidienne par compte (email, `end_date`=aujourd'hui)
    ET la plus forte variation sur 30j (`_biggest_variation`, historique, plus appelée dans le
    rapport actuel — Stéphane, 2026-07-27 : "sans LLM ça n'a aucun intérêt pour l'instant").
    Retourne {} si indisponible, jamais bloquant."""
    if not comptes:
        return {}
    payload = [{'etablissement': c['etablissement'], 'nature': c['nature'], 'titulaire': c['titulaire'], 'produit': c.get('produit'), 'devise': c['devise']} for c in comptes]
    try:
        r = requests.post(f'{LEDGER_API_URL}/api/ledger/comptes-solde', json={'orgId': org_id, 'comptes': payload, 'endDate': end_date}, timeout=15)
        r.raise_for_status()
        return {(item['etablissement'], item['nature'], item['titulaire'], item.get('produit')): item['solde'] for item in r.json().get('comptes', [])}
    except requests.RequestException:
        return {}


def _biggest_variation(org_id, comptes, days=30):
    """Compte avec la plus grosse variation absolue sur `days` jours — plus utilisée dans le
    rapport quotidien actuel (voir _build_patrimoine_payload), gardée pour une réintégration
    future avec un vrai commentaire LLM (Communicator/LLMPrecogn), pas un chiffre brut isolé."""
    if not comptes:
        return None
    end_date = (datetime.now() - timedelta(days=days)).strftime('%Y/%m/%d')
    past = _comptes_soldes_at(org_id, comptes, end_date)

    best = None
    for c in comptes:
        solde_avant = past.get((c['etablissement'], c['nature'], c['titulaire'], c.get('produit')))
        if solde_avant is None:
            continue
        variation = round(c['solde'] - solde_avant, 2)
        if abs(variation) >= 0.01 and (best is None or abs(variation) > abs(best['variation'])):
            best = {'nom': c['nom'], 'variation': variation, 'devise': c['devise']}
    return best


def _resolve_module(org_id, module):
    """Résout le module produit d'une org (ex. "suivre_mes_comptes") si l'appelant ne l'a pas
    fourni — lu directement depuis ledger_api (`module.json`), JAMAIS depuis `parent_org_id`
    (bug racine corrigé le 2026-07-27, voir ~/CLAUDE.md racine). Bug réel trouvé le 2026-07-28 :
    `daily_report.sh` n'a jamais transmis `module`, donc `_sync_org_comptes`/`_patrimoine_view_data`
    ne résolvaient JAMAIS aucun connector pour l'email quotidien — chaque compte y apparaissait
    "manuel" (même les 9 réellement automatiques) et surtout les comptes API n'étaient jamais
    resynchronisés avant de calculer le total envoyé par email, malgré la demande explicite de
    Stéphane le 2026-07-26 ("il faut bien sûr automatiser avant l'envoi d'email")."""
    if module:
        return module
    try:
        r = requests.get(f'{LEDGER_API_URL}/api/org/{org_id}/module', timeout=10)
        r.raise_for_status()
        return r.json().get('module')
    except requests.RequestException:
        return None


def _build_patrimoine_payload(org_id, module=None, prefix='Actif:Banque'):
    """Calcule tout ce qu'il faut pour un rapport patrimonial (liste des comptes + montants +
    variation quotidienne, total consolidé EUR). Factorisé (2026-07-27) : utilisé par
    /api/executor/daily-report (email) ET /api/executor/report-html (impression/PDF, voir §9
    ARCHITECTURE.md — email/impression sont des connectors de sortie interchangeables sur les
    MÊMES données, jamais des calculs dupliqués). Lève requests.RequestException telle quelle
    si Analyzor est injoignable (liste des comptes), à l'appelant de traduire en réponse HTTP
    appropriée — tout le reste (sync, owner, variation par compte) se dégrade gracieusement.

    Le total ET la variation globale sont calculés en SOMMANT la liste des comptes retournée
    (pas via `/api/ledger/patrimoine`, qui somme brut tout le préfixe `Actif:Banque` du grand
    livre) — bug réel trouvé le 2026-07-28 : deux écritures de test posées directement dans le
    journal pendant le développement du connector Powens (`Actif:Banque:Test:Jojo:épargne` et
    `Actif:Banque:Bcp:Jojo:épargne`, 2541€ + 2517€) n'ont jamais eu de brique Compte associée
    (ou une brique de test jamais nettoyée) et gonflaient silencieusement le total envoyé par
    email (310 438,84 € au lieu de ~305 327 €) sans jamais apparaître nulle part dans l'app pour
    que quelqu'un le remarque. Sommer la liste des VRAIES briques Compte garantit que le total
    affiché correspond TOUJOURS exactement à ce qui est listé juste en dessous — plus jamais un
    écart invisible entre les deux.

    Retourne (payload, owner_email) — owner_email est None si le propriétaire est introuvable
    (l'appelant décide alors s'il a un destinataire explicite de secours)."""
    module = _resolve_module(org_id, module)
    try:
        _sync_org_comptes(org_id, module)
    except requests.RequestException:
        pass

    today = datetime.now().strftime('%Y/%m/%d')
    comptes = _patrimoine_view_data(org_id, module)

    soldes_hier = _comptes_soldes_at(org_id, comptes, today)
    comptes_out = []
    for c in comptes:
        # Bug réel trouvé et corrigé le 2026-07-29 (retour de Stéphane : "il y avait pas le
        # compte bcp hier donc non ca ne fonctionne pas") : un compte SANS historique (créé le
        # jour même, ou dont c'est la 1re valeur jamais constatée) faisait `variation = 0.0`
        # par défaut — traité comme s'il avait TOUJOURS existé avec ce solde, donc invisible
        # dans le total ET dans le détail par compte alors qu'il s'agit d'une vraie augmentation
        # du patrimoine suivi. Un compte sans historique est traité comme parti de 0 (pas
        # "encore" suivi), jamais comme "sans changement" : sa première apparition affiche donc
        # sa vraie variation (son solde complet), cohérent avec le total global.
        solde_hier = soldes_hier.get((c['etablissement'], c['nature'], c['titulaire'], c.get('produit')))
        variation = round(c['solde'] - (solde_hier if solde_hier is not None else 0), 2)
        if abs(variation) < 0.01:
            variation = 0.0
        comptes_out.append({
            'nom': c['nom'], 'solde': c['solde'], 'devise': c['devise'], 'variation': variation,
            'numero': c.get('numero'),
        })
    comptes_out.sort(key=lambda c: (c['numero'] is None, c['numero'] if c['numero'] is not None else 0))

    owner = None
    try:
        owner = _org_owner_info(org_id)
    except requests.RequestException:
        pass

    # Total consolidé en EUR (2026-07-26, retour de Stéphane), affichage uniquement — voir
    # docstring de _fx_to_eur et _build_report_email. Même taux appliqué au total d'aujourd'hui
    # et à celui d'hier (dérivé en retranchant la variation de chaque compte) pour que la
    # variation reflète un vrai mouvement de solde, pas une fluctuation de change.
    totals = {}
    totals_hier = {}
    for c in comptes_out:
        totals[c['devise']] = totals.get(c['devise'], 0) + c['solde']
        totals_hier[c['devise']] = totals_hier.get(c['devise'], 0) + (c['solde'] - c['variation'])
    fx_rates = {d: _fx_to_eur(d) for d in set(totals) | set(totals_hier)}
    total_eur = sum(totals.get(d, 0) * fx_rates[d] for d in fx_rates)
    total_hier_eur = sum(totals_hier.get(d, 0) * fx_rates[d] for d in fx_rates)
    variation_eur = round(total_eur - total_hier_eur, 2)

    payload = {
        'date': today, 'comptes': comptes_out,
        'totalEur': round(total_eur, 2), 'variationEur': variation_eur,
        'navigatorUrl': f'{NAVIGATOR_URL}?orgId={org_id}',
        'ownerName': owner.get('name') if owner else None,
    }
    return payload, (owner.get('email') if owner else None)


@app.route('/api/executor/report-html', methods=['GET'])
def report_html():
    """Rapport patrimonial en HTML autonome, prêt à imprimer / exporter en PDF via le navigateur
    (bouton "Imprimer / PDF" du Navigator, section Outputs, 2026-07-27) — réutilise EXACTEMENT
    le même calcul et le même rendu que l'email quotidien (_build_patrimoine_payload +
    _build_report_email), jamais un rapport différent selon le canal de sortie.

    Query: ?orgId=...&module=..."""
    org_id = request.args.get('orgId', '')
    module = request.args.get('module')
    if not org_id:
        return jsonify({'success': False, 'error': 'orgId manquant'}), 400
    try:
        payload, _owner_email = _build_patrimoine_payload(org_id, module)
    except requests.RequestException as e:
        return jsonify({'success': False, 'error': f'ledger_api injoignable : {e}'}), 502
    _subject, _text_body, html_body = _build_report_email(payload)
    return jsonify({'success': True, 'html': html_body})


# ============================================================
# PLANNING DU RAPPORT (2026-07-29, retour de Stéphane : "pour l'envoi du mail tu peux laisser au
# user le choix de paramétrer la fréquence ? jour/hebdo/mensuel ? et l'heure d'envoi ?"). Fichier
# JSON local (pas un secret chiffré : rien de sensible ici, juste une préférence d'affichage —
# contrairement aux clés API de connectors) : {orgId: {frequency, hour, minute, weekday?,
# dayOfMonth?}}. Le cron (`smc-daily-report.timer`) tourne maintenant toutes les 30 min (heure
# de Paris) et appelle `/api/executor/daily-report/check-due`, qui n'envoie QUE pour les orgs
# dont l'horaire configuré correspond à maintenant — jamais un envoi par org codé en dur dans un
# timer séparé, ça ne passerait pas à l'échelle.
# ============================================================
REPORT_SCHEDULES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'report_schedules.json')
DEFAULT_REPORT_SCHEDULE = {'frequency': 'daily', 'hour': 0, 'minute': 0}


def _load_report_schedules():
    if not os.path.exists(REPORT_SCHEDULES_FILE):
        return {}
    with open(REPORT_SCHEDULES_FILE) as f:
        return json.load(f)


def _save_report_schedules(schedules):
    os.makedirs(os.path.dirname(REPORT_SCHEDULES_FILE), exist_ok=True)
    with open(REPORT_SCHEDULES_FILE, 'w') as f:
        json.dump(schedules, f, indent=2)


def _schedule_matches_now(schedule, now):
    """`now` : datetime avec tzinfo Europe/Paris. Créneaux de 30 min (0 ou 30) — cohérent avec
    la fréquence du cron, jamais plus précis que ce que le cron peut réellement vérifier."""
    slot = 30 if now.minute >= 30 else 0
    if now.hour != schedule.get('hour', 0) or slot != schedule.get('minute', 0):
        return False
    freq = schedule.get('frequency', 'daily')
    if freq == 'daily':
        return True
    if freq == 'weekly':
        return now.weekday() == schedule.get('weekday', 0)
    if freq == 'monthly':
        return now.day == schedule.get('dayOfMonth', 1)
    return False


@app.route('/api/executor/report-schedule', methods=['GET'])
def get_report_schedule():
    """Planning d'envoi configuré pour une org (ou le défaut si jamais configuré explicitement)
    — pour pré-remplir le formulaire self-service. Query: ?orgId=..."""
    org_id = request.args.get('orgId', '')
    if not org_id:
        return jsonify({'success': False, 'error': 'orgId manquant'}), 400
    schedule = _load_report_schedules().get(org_id, DEFAULT_REPORT_SCHEDULE)
    return jsonify({'success': True, 'schedule': schedule})


@app.route('/api/executor/report-schedule', methods=['POST'])
def set_report_schedule():
    """Enregistre le planning d'envoi d'une org. Body: {orgId, frequency: 'daily'|'weekly'|
    'monthly', hour: 0-23, minute: 0|30, weekday?: 0-6 (lundi=0, requis si weekly),
    dayOfMonth?: 1-28 (requis si monthly, plafonné à 28 pour éviter les mois courts)."""
    data = request.get_json() or {}
    org_id = data.get('orgId', '')
    if not org_id:
        return jsonify({'success': False, 'error': 'orgId manquant'}), 400

    frequency = data.get('frequency')
    if frequency not in ('daily', 'weekly', 'monthly'):
        return jsonify({'success': False, 'error': 'frequency invalide (daily/weekly/monthly)'}), 400
    try:
        hour = int(data.get('hour', 0))
        minute = int(data.get('minute', 0))
        if not (0 <= hour <= 23) or minute not in (0, 30):
            raise ValueError
    except (TypeError, ValueError):
        return jsonify({'success': False, 'error': 'Heure invalide (0-23) ou minute invalide (0 ou 30)'}), 400

    schedule = {'frequency': frequency, 'hour': hour, 'minute': minute}
    if frequency == 'weekly':
        try:
            weekday = int(data.get('weekday'))
            if not (0 <= weekday <= 6):
                raise ValueError
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': 'Jour de la semaine requis (0=lundi...6=dimanche)'}), 400
        schedule['weekday'] = weekday
    elif frequency == 'monthly':
        try:
            day_of_month = int(data.get('dayOfMonth'))
            if not (1 <= day_of_month <= 28):
                raise ValueError
        except (TypeError, ValueError):
            return jsonify({'success': False, 'error': 'Jour du mois requis (1-28)'}), 400
        schedule['dayOfMonth'] = day_of_month

    schedules = _load_report_schedules()
    schedules[org_id] = schedule
    _save_report_schedules(schedules)
    return jsonify({'success': True, 'schedule': schedule})


@app.route('/api/executor/daily-report/check-due', methods=['POST'])
def check_due_reports():
    """Appelé par le cron toutes les 30 min (Europe/Paris, voir smc-daily-report.timer) —
    envoie le rapport à chaque org dont l'horaire configuré (report-schedule) correspond à
    maintenant. Une org jamais configurée explicitement n'est PAS dans ce fichier et n'est donc
    jamais envoyée automatiquement ici (elle doit d'abord choisir un planning, self-service) —
    évite d'envoyer par défaut à une org qui n'a jamais rien demandé. Jamais bloquant : l'échec
    d'une org n'empêche pas les autres."""
    from zoneinfo import ZoneInfo
    now_paris = datetime.now(ZoneInfo('Europe/Paris'))
    schedules = _load_report_schedules()
    results = {}
    for org_id, schedule in schedules.items():
        if not _schedule_matches_now(schedule, now_paris):
            continue
        try:
            results[org_id] = _send_daily_report(org_id)
        except Exception as e:
            results[org_id] = {'success': False, 'error': str(e)}
    return jsonify({'success': True, 'results': results})


def _send_daily_report(org_id, module=None, prefix='Actif:Banque', to=None):
    """Calcule la position du patrimoine et l'envoie par email au propriétaire de l'org —
    premier connector de sortie (§9 ARCHITECTURE.md Suivre Mes Comptes), conçu comme un cas
    parmi d'autres (imprimer — voir /api/executor/report-html —, WhatsApp... à venir), jamais un
    canal en dur. Factorisé (2026-07-29) : appelé par la route HTTP `/api/executor/daily-report`
    (bouton "Renvoyer l'email maintenant" du Navigator) ET par `_check_due_reports` (planning
    configurable par org, voir report_schedule) — jamais deux implémentations différentes du
    même envoi. Retourne un dict {success, to?, payload?, sendError?, error?} — ne lève jamais,
    l'appelant décide de la traduction HTTP le cas échéant."""
    prefix = prefix or 'Actif:Banque'
    try:
        payload, owner_email = _build_patrimoine_payload(org_id, module, prefix)
    except requests.RequestException as e:
        return {'success': False, 'error': f'ledger_api injoignable : {e}'}

    to = to or owner_email
    if not to:
        return {'success': False, 'error': 'Aucun destinataire (propriétaire introuvable, "to" pas fourni)'}

    try:
        smtp_config = _org_smtp_secret(org_id)
    except requests.RequestException as e:
        return {'success': False, 'error': f'Analyzor injoignable (config SMTP) : {e}'}
    if not smtp_config:
        return {
            'success': False,
            'error': 'Aucune config email pour cette org — configure-la via Communicator : '
                     '"configure email host=... port=... user=... password=..."',
        }

    subject, text_body, html_body = _build_report_email(payload)
    try:
        _send_email_smtp(smtp_config, to, subject, text_body, html_body)
        send_result = {'success': True}
    except Exception as e:
        send_result = {'success': False, 'error': str(e)}

    log_to_journal(
        org_id, 'executor',
        f"Rapport quotidien envoyé à {to} : {_fmt_amount(payload['totalEur'], 'EUR')} "
        f"({_fmt_variation(payload['variationEur'], 'EUR')} depuis hier)",
        [f"{c['nom']} : {_fmt_amount(c['solde'], c['devise'])}" for c in payload['comptes']],
    )

    return {'success': send_result.get('success', False), 'to': to, 'payload': payload, 'sendError': send_result.get('error')}


@app.route('/api/executor/daily-report', methods=['POST'])
def daily_report():
    """Route HTTP pour `_send_daily_report` — voir sa docstring. Body: {orgId, module?,
    prefix?: "Actif:Banque", to?: email explicite (sinon propriétaire de l'org)}."""
    try:
        data = request.get_json() or {}
        org_id = data.get('orgId', '')
        if not org_id:
            return jsonify({'success': False, 'error': 'orgId manquant'}), 400
        result = _send_daily_report(org_id, data.get('module'), data.get('prefix'), data.get('to'))
        if result.get('to'):
            status = 200
        else:
            status = 502 if 'injoignable' in (result.get('error') or '') else 400
        return jsonify(result), status
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})


if __name__ == '__main__':
    # threaded=True (2026-07-28) : le serveur de dev Flask traite UNE requête à la fois par
    # défaut — un appel lent (sync avant email/rapport, 20-30s pour 19 comptes) bloquait TOUTE
    # autre requête pendant ce temps, cause plausible d'un "Erreur réseau" vu par Stéphane en
    # cliquant "Renvoyer l'email" pendant que d'autres appels tournaient en parallèle.
    app.run(host='0.0.0.0', port=8084, debug=False, threaded=True)
