"""Entity — base de tout objet du modèle métier Structory/PreCogn.

Décision structurante (Stéphane, 2026-08-01) : Structory n'est pas "une interface", c'est un
univers de concepts métier stables (Organisation, Journal, Compte, Connector, Flow, Rule,
Time, Document...) explorable par plusieurs VUES indépendantes (HTML, conversation, voix,
graphe, API...) sans jamais dupliquer la logique métier. Règle non négociable qui en découle :

    Une API externe (Enable Banking, Powens...) ne traverse JAMAIS directement l'application.
    Une vue ne lit JAMAIS directement une réponse API brute.

Chemin obligatoire : API externe -> Adaptateur -> Entity (ce module) -> Renderer.

Toute nouvelle "brique" métier (au sens Analyzor : Organisation, Compte, Rule...) qui a besoin
d'être représentée dans plusieurs vues DOIT être modélisée comme une sous-classe d'Entity ici
plutôt que comme un dict ad hoc construit à la volée dans une route Flask ou un fichier
Apps Script. Ce module ne connaît RIEN de HTML/JS/Tailwind/Navigator ni d'aucune API tierce —
uniquement la réalité métier elle-même.
"""


class Entity:
    """Base de tout objet du domaine. `entity_type` identifie le concept métier (jamais une
    technologie) — un renderer générique peut se brancher dessus ("comment afficher une
    Entity de type X ?") sans connaître chaque sous-classe à l'avance."""

    entity_type = "entity"

    def to_dict(self):
        """Sérialisation canonique, consommée par TOUS les renderers (HTML aujourd'hui,
        conversation/voix/graphe plus tard) — jamais un format différent par sous-classe qui
        forcerait chaque renderer à connaître les détails internes de chaque type d'Entity.
        Toujours inclure `entityType` (voir `entity_type`) pour qu'un renderer générique
        puisse dispatcher dessus."""
        raise NotImplementedError
