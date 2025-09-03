import os
import json
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
from datetime import datetime, date
import uuid
from werkzeug.utils import secure_filename
import logging
from dotenv import load_dotenv
import re

# Charger les variables d'environnement
load_dotenv()

# Imports des services
from services.session_service import SessionService
from services.file_processor import FileProcessorService
from services.file_manager import FileManager
from services.priority_processor import PriorityProcessor
from utils.validators import FileValidator
from utils.error_handler import APIErrorHandler, handle_api_errors
from utils.rate_limiter import apply_rate_limit
from database import db_manager

app = Flask(__name__)
CORS(app, expose_headers=["Content-Disposition"])


# Configuration améliorée
class Config:
    def __init__(self):
        self.UPLOAD_FOLDER = os.getenv("UPLOAD_FOLDER", "uploads")
        self.PROCESSED_FOLDER = os.getenv("PROCESSED_FOLDER", "processed")
        self.FINAL_FOLDER = os.getenv("FINAL_FOLDER", "final")
        self.ARCHIVE_FOLDER = os.getenv("ARCHIVE_FOLDER", "archive")
        self.LOG_FOLDER = os.getenv("LOG_FOLDER", "logs")
        self.MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", 16 * 1024 * 1024))
        self.SECRET_KEY = os.getenv("SECRET_KEY", "dev-key-change-in-production")

        # Créer les répertoires
        for folder in [
            self.UPLOAD_FOLDER,
            self.PROCESSED_FOLDER,
            self.FINAL_FOLDER,
            self.ARCHIVE_FOLDER,
            self.LOG_FOLDER,
        ]:
            os.makedirs(folder, exist_ok=True)


config = Config()
app.config.from_object(config)
app.secret_key = config.SECRET_KEY

# Configuration du logging améliorée
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(config.LOG_FOLDER, "inventory_processor.log")),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Initialisation des services
session_service = SessionService()
file_processor = FileProcessorService()
file_manager = FileManager(
    {
        "UPLOAD_FOLDER": config.UPLOAD_FOLDER,
        "PROCESSED_FOLDER": config.PROCESSED_FOLDER,
        "FINAL_FOLDER": config.FINAL_FOLDER,
        "ARCHIVE_FOLDER": config.ARCHIVE_FOLDER,
    }
)


# Classe de compatibilité (pour migration progressive)
class SageX3Processor:
    """
    Classe principale - utilise le traitement avec priorisation STRICTE LOTECART
    """

    def __init__(self):
        self.session_service = session_service
        self.file_processor = file_processor
        self.priority_processor = PriorityProcessor()

    def process_completed_file(self, session_id: str, completed_file_path: str, strategy: str = "FIFO"):
        """Traite le fichier Excel complété avec priorisation STRICTE LOTECART"""
        try:
            logger.info(f"🚀 DÉBUT TRAITEMENT FICHIER COMPLÉTÉ AVEC PRIORITÉ STRICTE LOTECART - Session: {session_id}")
            
            # Lire le fichier Excel complété
            completed_df = pd.read_excel(completed_file_path)

            # Validation des colonnes requises
            required_columns = [
                "Code Article",
                "Quantité Théorique",
                "Quantité Réelle",
                "Numéro Lot",
                "Numéro Inventaire",
            ]
            missing_columns = [
                col for col in required_columns if col not in completed_df.columns
            ]
            if missing_columns:
                raise ValueError(
                    f"Colonnes manquantes dans le fichier: {', '.join(missing_columns)}"
                )

            # Nettoyer les numéros de lot dans le fichier complété
            completed_df["Numéro Lot"] = (
                completed_df["Numéro Lot"].fillna("").astype(str).str.strip()
            )
            completed_df.loc[
                completed_df["Numéro Lot"].str.upper().isin(["NAN", "NULL", "NONE"]),
                "Numéro Lot",
            ] = ""

            # Conversion des types
            completed_df["Quantité Théorique"] = pd.to_numeric(
                completed_df["Quantité Théorique"], errors="coerce"
            )
            completed_df["Quantité Réelle"] = pd.to_numeric(
                completed_df["Quantité Réelle"], errors="coerce"
            )

            # Charger les données originales
            original_df = self.session_service.load_dataframe(session_id, "original_df")
            if original_df is None:
                raise ValueError("Données originales non trouvées pour la session")
            
            # NOUVEAU: Traitement avec priorisation STRICTE LOTECART
            processing_result = self.priority_processor.process_with_strict_priority(
                completed_df, original_df, strategy
            )
            
            # Extraire les résultats
            lotecart_summary = processing_result["lotecart_summary"]
            global_summary = processing_result["global_summary"]
            all_adjustments = processing_result["all_adjustments"]
            
            # Calculer les statistiques pour compatibilité
            total_discrepancy = sum(
                abs(adj.get("AJUSTEMENT", 0)) for adj in all_adjustments
            )
            adjusted_items_count = len(all_adjustments)


            # Sauvegarder les résultats dans les services
            self.session_service.save_dataframe(session_id, "completed_df", completed_df)
            
            # Sauvegarder les résultats du traitement prioritaire strict
            self.session_service.save_dataframe(
                session_id, "lotecart_candidates", 
                processing_result["lotecart_candidates"]
            )
            
            # Convertir les ajustements en DataFrame pour compatibilité
            if all_adjustments:
                distributed_df = pd.DataFrame(all_adjustments)
                self.session_service.save_dataframe(session_id, "distributed_df", distributed_df)

            # Mettre à jour la session en base
            self.session_service.update_session(
                session_id,
                total_discrepancy=total_discrepancy,
                adjusted_items_count=adjusted_items_count,
                strategy_used=strategy,
                status="processing_completed"
            )

            logger.info(
                f"✅ Fichier complété traité avec priorité STRICTE LOTECART pour session {session_id}: "
                f"{adjusted_items_count} ajustements totaux "
                f"({lotecart_summary.get('adjustments_created', 0)} LOTECART prioritaires validés)"
            )
            
            return processing_result

        except Exception as e:
            logger.error(f"❌ Erreur traitement fichier complété avec priorité stricte: {e}", exc_info=True)
            raise

    def distribute_discrepancies(self, session_id: str, strategy: str = "FIFO"):
        """OBSOLÈTE: Remplacé par le traitement prioritaire dans process_completed_file"""
        try:
            # Cette méthode est maintenant obsolète
            # Le traitement est fait directement dans process_completed_file avec priorisation
            logger.warning("⚠️ distribute_discrepancies est obsolète - utilisation du traitement prioritaire")
            
            # Récupérer les données du traitement prioritaire
            distributed_df = self.session_service.load_dataframe(session_id, "distributed_df")
            if distributed_df is None:
                raise ValueError("Aucune donnée de distribution trouvée - traitement prioritaire requis")

            logger.info(
                f"Distribution récupérée pour session {session_id}: {len(distributed_df)} ajustements"
            )
            return distributed_df

        except Exception as e:
            logger.error(f"Erreur distribution écarts: {e}")
            raise

    def _sort_lots_by_priority_and_strategy(
        self, lots_df: pd.DataFrame, strategy: str
    ) -> pd.DataFrame:
        """Trie les lots selon la priorité des types et la stratégie FIFO/LIFO"""
        # Définir l'ordre de priorité des types de lots (simplifié)
        type_priority = {"type1": 1, "type2": 2, "lotecart": 3, "unknown": 4}

        # Ajouter une colonne de priorité
        lots_df["priority"] = (
            lots_df.get("Type_Lot", "unknown").map(type_priority).fillna(4)
        )

        # Trier d'abord par priorité de type, puis par date selon la stratégie
        if strategy == "FIFO":
            # Type prioritaire d'abord, puis plus anciens d'abord
            sorted_lots = lots_df.sort_values(
                ["priority", "Date_Lot"], na_position="last"
            )
        else:  # LIFO
            # Type prioritaire d'abord, puis plus récents d'abord
            sorted_lots = lots_df.sort_values(
                ["priority", "Date_Lot"], ascending=[True, False], na_position="last"
            )

        # Pour les lots LOTECART, on ignore la date et on prend le premier disponible
        lotecart_lots = sorted_lots[sorted_lots.get("Type_Lot", "") == "lotecart"]
        other_lots = sorted_lots[sorted_lots.get("Type_Lot", "") != "lotecart"]

        # Recombiner : autres lots triés + lots LOTECART en premier disponible
        if not lotecart_lots.empty:
            result = pd.concat([other_lots, lotecart_lots], ignore_index=True)
        else:
            result = other_lots

        return result.drop("priority", axis=1, errors="ignore")

    def generate_final_file(self, session_id: str):
        """Génère le fichier CSV final avec traitement prioritaire LOTECART"""
        try:
            # Charger toutes les données nécessaires
            original_df = self.session_service.load_dataframe(session_id, "original_df")
            completed_df = self.session_service.load_dataframe(session_id, "completed_df")
            
            if original_df is None or completed_df is None:
                raise ValueError("Données manquantes pour générer le fichier final")

            # Récupérer les données de session depuis la base
            db_session_data = self.session_service.get_session_data(session_id)
            if not db_session_data:
                raise ValueError("Session non trouvée en base")
            
            # Récupérer les header_lines depuis la base
            import json
            header_lines = json.loads(db_session_data["header_lines"]) if db_session_data["header_lines"] else []

            # Construire le nom du fichier
            original_filename = db_session_data["original_filename"]
            base_name = os.path.splitext(original_filename)[0]
            final_filename = f"{base_name}_corrige_{session_id}.csv"
            final_file_path = os.path.join(config.FINAL_FOLDER, final_filename)

            # Récupérer les données du traitement prioritaire
            distributed_df = self.session_service.load_dataframe(session_id, "distributed_df")
            if distributed_df is None:
                logger.warning("⚠️ Pas de données distribuées - le traitement prioritaire n'a pas été effectué")
                return distributed_df  # Retourner pour compatibilité

            logger.info("✅ Données de distribution récupérées - traitement prioritaire déjà effectué")
            return distributed_df
        except Exception as e:
            logger.error(f"Erreur génération fichier final: {e}")
            raise
    
    def generate_coherent_final_file(self, session_id: str):
        """Génère le fichier final avec cohérence GARANTIE"""
        try:
            logger.info(f"🎯 GÉNÉRATION FICHIER FINAL COHÉRENT - Session: {session_id}")
            
            # Charger toutes les données nécessaires
            original_df = self.session_service.load_dataframe(session_id, "original_df")
            completed_df = self.session_service.load_dataframe(session_id, "completed_df")
            
            if original_df is None or completed_df is None:
                raise ValueError("Données manquantes pour générer le fichier final")

            # Récupérer les données de session depuis la base
            db_session_data = self.session_service.get_session_data(session_id)
            if not db_session_data:
                raise ValueError("Session non trouvée en base")
            
            # Récupérer les header_lines depuis la base
            import json
            header_lines = json.loads(db_session_data["header_lines"]) if db_session_data["header_lines"] else []

            # Construire le nom du fichier
            original_filename = db_session_data["original_filename"]
            base_name = os.path.splitext(original_filename)[0]
            final_filename = f"{base_name}_corrige_{session_id}.csv"
            final_file_path = os.path.join(config.FINAL_FOLDER, final_filename)

            # Utiliser le processeur prioritaire strict pour générer le fichier
            final_path, generation_summary = self.priority_processor.generate_coherent_final_file(
                session_id, original_df, completed_df, header_lines, final_file_path
            )

            # Mettre à jour la session
            self.session_service.update_session(
                session_id, 
                final_file_path=final_path,
                status="completed"
            )

            # Vérification finale du fichier généré
            self._verify_final_file_coherence(final_path)
            
            logger.info(f"✅ Fichier final COHÉRENT généré avec priorité STRICTE LOTECART: {final_path}")
            logger.info(f"📊 Résumé génération: {generation_summary}")
            
            return final_path

        except Exception as e:
            logger.error(f"❌ Erreur génération fichier final cohérent: {e}", exc_info=True)
            raise
    
    def _verify_final_file_coherence(self, final_file_path: str):
        """Vérifie la cohérence du fichier final généré avec focus LOTECART"""
        try:
            logger.info(f"🔍 VÉRIFICATION COHÉRENCE FICHIER FINAL: {final_file_path}")
            
            lotecart_count = 0
            lotecart_coherent = 0
            total_lines = 0
            issues = []
            
            with open(final_file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    if line.startswith('S;'):
                        total_lines += 1
                        if 'LOTECART' in line:
                            lotecart_count += 1
                            parts = line.strip().split(';')
                            if len(parts) > 14:
                                article = parts[8]
                                qte_theo = parts[5]
                                qte_reelle = parts[6]
                                indicateur = parts[7]
                                
                                # Vérification stricte LOTECART
                                try:
                                    f_val = float(qte_theo)
                                    g_val = float(qte_reelle)
                                    
                                    if (indicateur == '2' and 
                                        abs(f_val - g_val) < 0.001 and 
                                        f_val > 0 and g_val > 0):
                                        lotecart_coherent += 1
                                        logger.debug(f"✅ LOTECART COHÉRENT ligne {line_num}: {article} - F={qte_theo}, G={qte_reelle}")
                                    else:
                                        issues.append(f"❌ LOTECART INCOHÉRENT ligne {line_num}: {article} - F={qte_theo}, G={qte_reelle}, Ind={indicateur}")
                                        logger.error(f"❌ LOTECART INCOHÉRENT ligne {line_num}: {article} - F={qte_theo}, G={qte_reelle}, Ind={indicateur}")
                                except ValueError:
                                    issues.append(f"❌ QUANTITÉS NON NUMÉRIQUES ligne {line_num}: {article}")
                                    logger.error(f"❌ QUANTITÉS NON NUMÉRIQUES ligne {line_num}: {article}")
            
            # Résumé de vérification
            coherence_percentage = (lotecart_coherent / lotecart_count * 100) if lotecart_count > 0 else 100
            
            logger.info(
                f"📊 VÉRIFICATION TERMINÉE: "
                f"{lotecart_count} lignes LOTECART sur {total_lines} lignes S - "
                f"Cohérentes: {lotecart_coherent}/{lotecart_count} ({coherence_percentage:.1f}%)"
            )
            
            if issues:
                logger.error(f"❌ {len(issues)} problème(s) de cohérence détecté(s):")
                for issue in issues[:5]:  # Afficher max 5 problèmes
                    logger.error(f"   {issue}")
                if len(issues) > 5:
                    logger.error(f"   ... et {len(issues) - 5} autres problèmes")
            
            if coherence_percentage < 100:
                raise ValueError(f"FICHIER FINAL INCOHÉRENT: {coherence_percentage:.1f}% de cohérence LOTECART")
            
            logger.info("✅ FICHIER FINAL PARFAITEMENT COHÉRENT")
            
        except Exception as e:
            logger.error(f"❌ Erreur vérification cohérence fichier final: {e}", exc_info=True)
            raise


# Initialisation du processeur
processor = SageX3Processor()


# Endpoints API
@app.route("/api/upload", methods=["POST"])
@apply_rate_limit("upload")
@handle_api_errors("file_upload")
def upload_file():
    """Endpoint amélioré pour l'upload initial d'un fichier Sage X3"""
    if "file" not in request.files:
        return jsonify({"error": "Aucun fichier fourni"}), 400

    file = request.files["file"]
    if not file.filename:
        return jsonify({"error": "Nom de fichier vide"}), 400

    file_extension = os.path.splitext(file.filename)[1].lower()
    if file_extension not in [".csv", ".xlsx", ".xls"]:
        return (
            jsonify({"error": "Format non supporté. Seuls CSV et XLSX sont acceptés"}),
            400,
        )

    # Validation sécurisée du fichier
    is_valid, error_msg = FileValidator.validate_file_security(
        file, config.MAX_FILE_SIZE
    )
    if not is_valid:
        return jsonify({"error": error_msg}), 400

    session_creation_timestamp = datetime.now()
    filepath = None

    try:
        # Créer la session en base de données
        session_id = session_service.create_session(
            original_filename=file.filename,
            original_file_path="",  # Sera mis à jour après sauvegarde
            status="uploading",
        )

        filename_on_disk = secure_filename(f"{session_id}_{file.filename}")
        filepath = os.path.join(config.UPLOAD_FOLDER, filename_on_disk)
        file.save(filepath)

        # Mettre à jour le chemin du fichier
        session_service.update_session(session_id, original_file_path=filepath)

        # Traitement du fichier
        is_valid, result_data, headers, inventory_date = (
            file_processor.validate_and_process_sage_file(
                filepath, file_extension, session_creation_timestamp
            )
        )

        if not is_valid:
            if os.path.exists(filepath):
                os.remove(filepath)
            session_service.delete_session(session_id)
            return jsonify({"error": str(result_data)}), 400

        original_df = result_data

        # Agrégation
        aggregated_df = file_processor.aggregate_data(original_df)

        # Génération du template
        template_file_path = file_processor.generate_template(
            aggregated_df, session_id, config.PROCESSED_FOLDER
        )

        # Mise à jour de la session
        session_service.update_session(
            session_id,
            template_file_path=template_file_path,
            status="template_generated",
            inventory_date=inventory_date,
            nb_articles=len(aggregated_df),
            nb_lots=len(original_df),
            total_quantity=float(aggregated_df["Quantite_Theorique_Totale"].sum()),
            header_lines=json.dumps(headers),
        )

        # Sauvegarder les DataFrames de manière persistante
        session_service.save_dataframe(session_id, "original_df", original_df)
        session_service.save_dataframe(session_id, "aggregated_df", aggregated_df)

        # Sauvegarder aussi dans le stockage persistant
        session_service.save_dataframe(session_id, "original_df", original_df)
        session_service.save_dataframe(session_id, "aggregated_df", aggregated_df)

        return jsonify(
            {
                "success": True,
                "session_id": session_id,
                "template_url": f"/api/download/template/{session_id}",
                "stats": {
                    "nb_articles": len(aggregated_df),
                    "total_quantity": float(
                        aggregated_df["Quantite_Theorique_Totale"].sum()
                    ),
                    "nb_lots": len(original_df),
                    "inventory_date": (
                        inventory_date.isoformat() if inventory_date else None
                    ),
                },
            }
        )

    except Exception as e:
        logger.error(f"Erreur upload: {e}", exc_info=True)
        if filepath and os.path.exists(filepath):
            os.remove(filepath)
        return jsonify({"error": "Erreur interne du serveur"}), 500


@app.route("/api/process", methods=["POST"])
@apply_rate_limit("upload")
@handle_api_errors("file_processing")
def process_completed_file_route():
    """Endpoint amélioré pour traiter le fichier complété"""
    if "file" not in request.files or "session_id" not in request.form:
        return jsonify({"error": "Paramètres manquants"}), 400

    try:
        session_id = request.form["session_id"]
        file = request.files["file"]
        strategy = request.form.get("strategy", "FIFO")

        # Vérifier que la session existe
        session_data = session_service.get_session_data(session_id)
        if not session_data:
            return jsonify({"error": "Session non trouvée"}), 404

        if not file.filename.lower().endswith((".xlsx", ".xls")):
            return jsonify({"error": "Seuls les fichiers Excel sont acceptés"}), 400

        # Validation du fichier complété
        temp_filepath = os.path.join(
            config.PROCESSED_FOLDER, f"temp_{session_id}_{file.filename}"
        )
        file.save(temp_filepath)

        is_valid, validation_msg, errors = file_processor.validate_completed_template(
            temp_filepath
        )
        if not is_valid:
            os.remove(temp_filepath)
            return jsonify({"error": validation_msg, "details": errors}), 400

        filename_on_disk = secure_filename(f"completed_{session_id}_{file.filename}")
        filepath = os.path.join(config.PROCESSED_FOLDER, filename_on_disk)
        os.rename(temp_filepath, filepath)

        # NOUVEAU: Traitement avec priorisation STRICTE LOTECART
        processing_result = processor.process_completed_file(session_id, filepath, strategy)
        
        # Génération du fichier final avec cohérence garantie
        final_file_path = processor.generate_coherent_final_file(session_id)

        # Mise à jour de la session en base
        session_service.update_session(
            session_id,
            completed_file_path=filepath,
            final_file_path=final_file_path,
            status="completed",
            strategy_used=strategy,
        )

        # Récupérer les données depuis les services
        session_data = session_service.get_session_data(session_id)
        if not session_data:
            return jsonify({"error": "Session non trouvée"}), 404

        return jsonify(
            {
                "success": True,
                "final_url": f"/api/download/final/{session_id}",
                "stats": {
                    "total_discrepancy": session_data.get("total_discrepancy", 0),
                    "adjusted_items": session_data.get("adjusted_items_count", 0),
                    "strategy_used": session_data.get("strategy_used", "N/A"),
                    "processing_mode": "STRICT_PRIORITY_LOTECART_COHERENT",
                    "lotecart_count": processing_result.get("lotecart_summary", {}).get("adjustments_created", 0),
                    "quality_score": processing_result.get("lotecart_summary", {}).get("quality_score", 0)
                },
            }
        )

    except ValueError as e:
        logger.error(f"Erreur validation: {e}")
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        logger.error(f"Erreur traitement: {e}", exc_info=True)
        return jsonify({"error": "Erreur interne du serveur"}), 500


@app.route("/api/download/<file_type>/<session_id>", methods=["GET"])
@handle_api_errors("file_download")
def download_file(file_type: str, session_id: str):
    """Endpoint de téléchargement amélioré"""
    try:
        session_data = session_service.get_session_data(session_id)
        if not session_data:
            return jsonify({"error": "Session non trouvée"}), 404

        filepath = None
        download_name = None
        mimetype = None

        if file_type == "template":
            filepath = session_data["template_file_path"]
            if not filepath:
                return jsonify({"error": "Template non généré"}), 404
            download_name = os.path.basename(filepath)
            mimetype = (
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        elif file_type == "final":
            filepath = session_data["final_file_path"]
            if not filepath:
                return jsonify({"error": "Fichier final non généré"}), 404
            download_name = os.path.basename(filepath)
            mimetype = "text/csv"
        else:
            return jsonify({"error": "Type de fichier invalide"}), 400

        if not os.path.exists(filepath):
            return jsonify({"error": "Fichier non trouvé sur le serveur"}), 404

        return send_file(
            filepath, as_attachment=True, download_name=download_name, mimetype=mimetype
        )

    except Exception as e:
        logger.error(f"Erreur téléchargement: {e}", exc_info=True)
        return jsonify({"error": "Erreur interne du serveur"}), 500


@app.route("/api/sessions", methods=["GET"])
def list_sessions():
    """Liste les sessions avec pagination"""
    try:
        limit = int(request.args.get("limit", 50))
        include_expired = request.args.get("include_expired", "false").lower() == "true"

        sessions_list = session_service.list_sessions(
            limit=limit, include_expired=include_expired
        )

        return jsonify({"sessions": sessions_list})

    except Exception as e:
        logger.error(f"Erreur listage sessions: {e}", exc_info=True)
        return jsonify({"error": "Erreur interne du serveur"}), 500


@app.route("/api/sessions/<session_id>", methods=["DELETE"])
def delete_session(session_id: str):
    """Supprime une session"""
    try:
        success = session_service.delete_session(session_id)
        if success:
            # Nettoyer les données de session
            session_service.cleanup_session_data(session_id)
            return jsonify({"success": True})
        else:
            return jsonify({"error": "Session non trouvée"}), 404
    except Exception as e:
        logger.error(f"Erreur suppression session: {e}")
        return jsonify({"error": "Erreur interne du serveur"}), 500


@app.route("/api/analyze/<session_id>", methods=["GET"])
def analyze_file_format(session_id: str):
    """Endpoint pour analyser le format d'un fichier uploadé"""
    try:
        session = session_service.get_session(session_id)
        if not session:
            return jsonify({"error": "Session non trouvée"}), 404

        filepath = session.original_file_path
        if not os.path.exists(filepath):
            return jsonify({"error": "Fichier non trouvé"}), 404

        format_detected, format_msg, format_info = file_processor.detect_file_format(
            filepath
        )

        return jsonify(
            {
                "success": format_detected,
                "message": format_msg,
                "format_info": format_info,
                "expected_format": {
                    "columns_required": len(file_processor.SAGE_COLUMN_NAMES_ORDERED),
                    "column_names": file_processor.SAGE_COLUMN_NAMES_ORDERED,
                    "expected_line_types": ["E", "L", "S"],
                },
            }
        )

    except Exception as e:
        logger.error(f"Erreur analyse format: {e}")
        return jsonify({"error": "Erreur interne du serveur"}), 500


@app.route("/api/health", methods=["GET"])
def health_check():
    """Endpoint de santé amélioré"""
    try:
        db_healthy = db_manager.health_check()
        sessions_count = len(session_service.list_sessions(limit=1000))

        status = "healthy" if db_healthy else "degraded"

        return jsonify(
            {
                "status": status,
                "timestamp": datetime.now().isoformat(),
                "database": "healthy" if db_healthy else "error",
                "active_sessions_count": sessions_count,
            }
        )
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return (
            jsonify(
                {
                    "status": "error",
                    "timestamp": datetime.now().isoformat(),
                    "error": str(e),
                }
            ),
            500,
        )


# Tâche de nettoyage (à exécuter périodiquement)
@app.route("/api/cleanup", methods=["POST"])
def cleanup_sessions():
    """Nettoie les sessions expirées"""
    try:
        hours = int(request.json.get("hours", 24))

        # Nettoyage des sessions en base
        cleaned_sessions = session_service.cleanup_expired_sessions(hours)

        # Nettoyage des fichiers anciens
        days_old = int(request.json.get("days_old", 7))
        file_stats = file_manager.cleanup_old_files(days_old)

        return jsonify(
            {
                "cleaned_sessions": cleaned_sessions,
                "cleaned_files": file_stats,
                "total_files_cleaned": sum(file_stats.values()),
            }
        )
    except Exception as e:
        logger.error(f"Erreur nettoyage: {e}")
        return jsonify({"error": "Erreur nettoyage"}), 500


@app.route("/api/archive/<session_id>", methods=["POST"])
def archive_session(session_id: str):
    """Archive une session et ses fichiers"""
    try:
        # Vérifier que la session existe
        session_data = session_service.get_session_data(session_id)
        if not session_data:
            return jsonify({"error": "Session non trouvée"}), 404

        # Archiver les fichiers
        success = file_manager.archive_session_files(
            session_id, session_data.get("created_at")
        )

        if success:
            # Marquer la session comme archivée
            session_service.update_session(session_id, status="archived")
            return jsonify({"success": True, "message": "Session archivée avec succès"})
        else:
            return jsonify({"error": "Erreur lors de l'archivage"}), 500

    except Exception as e:
        logger.error(f"Erreur archivage session {session_id}: {e}")
        return jsonify({"error": "Erreur interne du serveur"}), 500


@app.route("/api/stats/files", methods=["GET"])
def get_file_stats():
    """Retourne les statistiques des fichiers"""
    try:
        stats = file_manager.get_folder_stats()
        return jsonify({"folder_stats": stats})
    except Exception as e:
        logger.error(f"Erreur stats fichiers: {e}")
        return jsonify({"error": "Erreur interne du serveur"}), 500


if __name__ == "__main__":
    logger.info("Démarrage de l'application Moulinette Sage X3")
    app.run(host="0.0.0.0", port=5000, debug=True)
