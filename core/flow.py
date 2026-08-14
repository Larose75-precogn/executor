"""Flow / FlowMember / FlowRelation — objets du domaine (voir core/entity.py pour la règle
générale).

Révision (Stéphane, 2026-08-01, 2e passe) : "Node" venait des outils de graphe (Node-RED, React
Flow...) — un mot de REPRÉSENTATION, pas de métier. Un Connector n'est pas un nœud, c'est un
Connector ; une Rule n'est pas un nœud, c'est une Rule. Un Flow ne connaît maintenant que deux
choses, toutes deux nommées en vocabulaire métier :

    - ses MEMBRES (`members`, ex. FlowMember) : la participation d'un atome du domaine
      (Connector/Rule/Time/Journal/Account/...) à CE Flow précis, à cet instant.
    - leurs RELATIONS (`relations`, ex. FlowRelation) : comment ces membres s'articulent entre
      eux, avec un TYPE sémantique (pas juste un ordre d'affichage).

Le graphe (qui pointe vers qui, dans quel ordre) devient une VUE dérivée de ces deux listes,
jamais la forme canonique elle-même — une vue graphique en fait un diagramme, une vue
conversationnelle en fait des phrases, une vue vocale les énonce dans l'ordre logique. Le Flow
lui-même ne sait dessiner ni graphe ni pipeline.
"""

from .entity import Entity

MEMBER_PENDING = "pending"
MEMBER_ACTIVE = "active"
MEMBER_SUCCESS = "success"
MEMBER_ERROR = "error"

RELATION_THEN = "then"  # enchaînement séquentiel simple — cas d'un Flow bancaire aujourd'hui.
# Types futurs, une fois Rule/Time/Journal impliqués dans un Flow non-linéaire :
# "triggers" (Time déclenche), "uses_rule" (Rule appliquée), "feeds" (alimente un Journal)...

# Membres canoniques du Flow "connexion bancaire" (Suivre Mes Comptes) — un seul endroit qui
# connaît cette séquence, jamais recopiée côté client.
BANK_CONNECTION_MEMBER_DEFS = [
    ("banque", "🏦", "Banque"),
    ("connecteur", "🔌", "Connecteur"),
    ("auth", "🔐", "Auth"),
    ("comptes", "📥", "Comptes"),
    ("solde", "💰", "Solde"),
]


class FlowMember(Entity):
    """La participation d'un atome du domaine (`atom`, une autre Entity : Connector/Rule/Time/
    Journal/Account/...) à ce Flow précis, à cet instant. Le membre N'EST PAS l'atome lui-même :
    un Connector existe indépendamment de tout Flow ; un FlowMember porte juste une référence
    vers lui + un statut CONTEXTUEL à CETTE exécution (pending/active/success/error), qui n'a de
    sens que dans le cadre de ce Flow — jamais une propriété permanente de l'atome référencé.
    `atom` peut être None (membre purement structurel, ex. "Banque" avant qu'un vrai Connector
    ne soit résolu)."""

    entity_type = "flow_member"

    def __init__(self, member_id, icon, label, status=MEMBER_PENDING, atom=None):
        self.member_id = member_id
        self.icon = icon
        self.label = label
        self.status = status
        self.atom = atom

    def to_dict(self):
        return {
            "entityType": self.entity_type,
            "id": self.member_id,
            "icon": self.icon,
            "label": self.label,
            "status": self.status,
            "atomType": self.atom.entity_type if self.atom else None,
            "atom": self.atom.to_dict() if self.atom else None,
        }


class FlowRelation(Entity):
    """Comment deux membres d'un Flow s'articulent — `relation_type` porte le SENS (pas juste un
    ordre d'affichage) : "then" (enchaînement simple, cas d'un Flow bancaire) aujourd'hui,
    "triggers"/"uses_rule"/"feeds" demain pour un Flow avec Rule/Time non-linéaires. Une vue
    graphique en fait une flèche, une vue conversationnelle peut l'ignorer et juste parcourir
    les membres dans un ordre logique."""

    entity_type = "flow_relation"

    def __init__(self, source, target, relation_type=RELATION_THEN):
        self.source = source
        self.target = target
        self.relation_type = relation_type

    def to_dict(self):
        return {
            "entityType": self.entity_type,
            "source": self.source,
            "target": self.target,
            "type": self.relation_type,
        }


class Flow(Entity):
    entity_type = "flow"

    def __init__(self, members, relations=None, error_message=None):
        self.members = members
        self.relations = relations if relations is not None else self._sequential_relations(members)
        self.error_message = error_message

    @staticmethod
    def _sequential_relations(members):
        """Repli par défaut : un enchaînement purement séquentiel (chaque membre relié au
        suivant par une relation "then") — exactement ce qu'un Flow bancaire est aujourd'hui,
        mais explicite/dérivé plutôt qu'implicite dans l'ordre d'une liste."""
        return [FlowRelation(members[i].member_id, members[i + 1].member_id) for i in range(len(members) - 1)]

    def set_status(self, member_id, status):
        for member in self.members:
            if member.member_id == member_id:
                member.status = status
                return

    def to_dict(self):
        return {
            "entityType": self.entity_type,
            "members": [m.to_dict() for m in self.members],
            "relations": [r.to_dict() for r in self.relations],
            "errorMessage": self.error_message,
        }

    @classmethod
    def bank_connection(cls, connecteur_label=None):
        """Un Flow de connexion bancaire vierge (tous les membres 'pending') — voir
        BANK_CONNECTION_MEMBER_DEFS. `connecteur_label` remplace le libellé générique une fois
        le vrai connector connu (ex. "Powens"/"Enable Banking"), avant même qu'il ne soit résolu
        en tant qu'atome réel (voir bank_connection_completed)."""
        members = []
        for member_id, icon, label in BANK_CONNECTION_MEMBER_DEFS:
            if member_id == "connecteur" and connecteur_label:
                label = connecteur_label
            members.append(FlowMember(member_id, icon, label))
        return cls(members)

    @classmethod
    def bank_connection_completed(cls, etablissement, connector):
        """Un Flow de connexion bancaire déjà réussi (tous les membres 'success') — utilisé pour
        la confirmation statique d'un compte déjà automatisé. Le `connector` RÉEL (Entity, voir
        core/connector.py) est attaché comme atome du membre 'connecteur' — un renderer qui a
        besoin de plus de détail que le simple libellé (ex. afficher aussi `brickId`) peut aller
        le chercher là, jamais reconstruit indépendamment."""
        flow = cls.bank_connection(connector.friendly_name)
        flow.members[0].label = etablissement
        for member in flow.members:
            member.status = MEMBER_SUCCESS
            if member.member_id == "connecteur":
                member.atom = connector
        return flow
