import os
import pandas as pd
from typing import Tuple, Union, List
from werkzeug.utils import secure_filename
import logging

# Import conditionnel de python-magic
try:
    import magic
    MAGIC_AVAILABLE = True
except ImportError:
    MAGIC_AVAILABLE = False
    magic = None

logger = logging.getLogger(__name__)

class FileValidator:
    """Validateur de fichiers avec sécurité renforcée"""
    
    ALLOWED_MIME_TYPES = {
        'text/csv': ['.csv'],
        'application/vnd.ms-excel': ['.xls'],
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'],
        'text/plain': ['.csv'],  # Parfois les CSV sont détectés comme text/plain
        'application/zip': ['.xlsx'],  # Les fichiers XLSX sont des archives ZIP
        'application/x-zip-compressed': ['.xlsx']  # Variante de détection ZIP
    }
    
    ALLOWED_EXTENSIONS = {'.csv', '.xlsx', '.xls'}
    
    @staticmethod
    def _validate_extension_only(file_ext: str) -> Tuple[bool, str]:
        """Validation par extension uniquement (fallback)"""
        if file_ext not in FileValidator.ALLOWED_EXTENSIONS:
            return False, f"Extension {file_ext} non autorisée. Extensions autorisées: {', '.join(FileValidator.ALLOWED_EXTENSIONS)}"
        return True, "Extension valide"
    
    @staticmethod
    def _validate_csv_content(file) -> Tuple[bool, str]:
        """Validation basique du contenu CSV"""
        try:
            # Lire les premières lignes pour vérifier la structure
            file.seek(0)
            first_lines = []
            for i, line in enumerate(file):
                if i >= 10:  # Lire max 10 lignes
                    break
                if isinstance(line, bytes):
                    line = line.decode('utf-8', errors='ignore')
                first_lines.append(line.strip())
            
            file.seek(0)  # Remettre le curseur au début
            
            if not first_lines:
                return False, "Fichier CSV vide"
            
            # Vérifier qu'il y a des lignes E; ou L; ou S;
            has_sage_format = any(
                line.startswith(('E;', 'L;', 'S;')) 
                for line in first_lines
            )
            
            if not has_sage_format:
                return False, "Format Sage X3 non détecté (aucune ligne E;, L; ou S; trouvée)"
            
            # Vérifier qu'il n'y a pas de caractères suspects
            suspicious_patterns = [
                b'<script',
                b'javascript:',
                b'<?php',
                b'<%',
                b'exec(',
                b'eval(',
            ]
            
            file.seek(0)
            content_sample = file.read(1024)  # Lire 1KB
            file.seek(0)
            
            if isinstance(content_sample, str):
                content_sample = content_sample.encode('utf-8')
            
            for pattern in suspicious_patterns:
                if pattern in content_sample.lower():
                    return False, "Contenu suspect détecté dans le fichier"
            
            return True, "Contenu CSV valide"
            
        except Exception as e:
            logger.warning(f"Erreur validation contenu CSV: {e}")
            return True, "Validation contenu ignorée"  # Ne pas bloquer en cas d'erreur
    
    @staticmethod
    def validate_file_security(file, max_size: int) -> Tuple[bool, str]:
        """Validation sécurisée du fichier"""
        try:
            # Vérification de la taille
            file.seek(0, os.SEEK_END)
            file_size = file.tell()
            file.seek(0)
            
            if file_size > max_size:
                return False, f"Fichier trop volumineux ({file_size / 1024 / 1024:.1f}MB > {max_size / 1024 / 1024:.1f}MB)"
            
            if file_size == 0:
                return False, "Fichier vide"
            
            # Vérification de la taille minimale (éviter les fichiers trop petits)
            if file_size < 10:  # Moins de 10 bytes
                return False, "Fichier trop petit pour être valide"
            
            # Vérification du nom de fichier
            if not file.filename:
                return False, "Nom de fichier manquant"
            
            filename = secure_filename(file.filename)
            if not filename:
                return False, "Nom de fichier invalide"
            
            # Vérification de l'extension
            file_ext = os.path.splitext(filename)[1].lower()
            if not file_ext:
                return False, "Extension de fichier manquante"
            
            # Vérification du type MIME (si python-magic est disponible)
            if MAGIC_AVAILABLE:
                try:
                    file_content = file.read(1024)  # Lire les premiers 1024 bytes
                    file.seek(0)  # Remettre le curseur au début
                    
                    mime_type = magic.from_buffer(file_content, mime=True)
                    
                    # Validation spéciale pour les fichiers XLSX
                    if file_ext == '.xlsx':
                        # Les fichiers XLSX peuvent être détectés comme ZIP ou comme leur type MIME correct
                        allowed_xlsx_mimes = [
                            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                            'application/zip',
                            'application/x-zip-compressed'
                        ]
                        if mime_type not in allowed_xlsx_mimes:
                            return False, f"Type MIME non autorisé pour fichier XLSX: {mime_type}"
                    elif mime_type not in FileValidator.ALLOWED_MIME_TYPES:
                        return False, f"Type de fichier non autorisé: {mime_type}"
                    else:
                        # Vérification normale pour les autres types
                        allowed_extensions = FileValidator.ALLOWED_MIME_TYPES[mime_type]
                        if file_ext not in allowed_extensions:
                            return False, f"Extension {file_ext} non compatible avec le type {mime_type}"
                            
                except Exception as e:
                    logger.warning(f"Erreur lors de la détection MIME: {e}")
                    # Fallback sur l'extension uniquement en cas d'erreur
                    return FileValidator._validate_extension_only(file_ext)
            else:
                # python-magic non disponible, validation par extension uniquement
                logger.info("python-magic non disponible, validation par extension uniquement")
                return FileValidator._validate_extension_only(file_ext)
            
            # Validation supplémentaire du contenu pour les fichiers CSV
            if file_ext == '.csv':
                is_valid_csv, csv_error = FileValidator._validate_csv_content(file)
                if not is_valid_csv:
                    return False, csv_error
            
            return True, "Fichier valide"
            
        except Exception as e:
            logger.error(f"Erreur validation fichier: {str(e)}")
            from utils.error_handler import ErrorSanitizer
            sanitized_error = ErrorSanitizer.sanitize_error_message(e, include_type=False)
            return False, f"Erreur de validation: {sanitized_error}"

class DataValidator:
    """Validateur de données métier"""
    
    @staticmethod
    def validate_sage_structure(df: pd.DataFrame, required_columns: dict) -> Tuple[bool, str]:
        """Valide la structure des données Sage X3"""
        try:
            # Vérification du nombre de colonnes
            max_col_needed = max(required_columns.values())
            if df.shape[1] <= max_col_needed:
                return False, f"Nombre de colonnes insuffisant. Minimum {max_col_needed + 1} colonnes requises, {df.shape[1]} trouvées"
            
            # Vérification des données quantité
            qty_col = required_columns['QUANTITE']
            quantities = pd.to_numeric(df.iloc[:, qty_col], errors='coerce')
            
            if quantities.isna().any():
                invalid_count = quantities.isna().sum()
                return False, f"{invalid_count} valeurs de quantité invalides détectées"
            
            if (quantities < 0).any():
                negative_count = (quantities < 0).sum()
                return False, f"{negative_count} quantités négatives détectées"
            
            # Vérification des codes articles
            article_col = required_columns['CODE_ARTICLE']
            articles = df.iloc[:, article_col].astype(str)
            
            if articles.str.strip().eq('').any():
                empty_count = articles.str.strip().eq('').sum()
                return False, f"{empty_count} codes articles vides détectés"
            
            return True, "Structure valide"
            
        except Exception as e:
            return False, f"Erreur de validation des données: {str(e)}"
    
    @staticmethod
    def validate_template_completion(df: pd.DataFrame) -> Tuple[bool, str, List[str]]:
        """Valide le fichier template complété"""
        errors = []
        
        # Colonnes requises
        required_columns = {'Numéro Session', 'Numéro Inventaire', 'Code Article', 'Quantité Théorique', 'Quantité Réelle', 'Numéro Lot'}
        missing_columns = required_columns - set(df.columns)
        
        if missing_columns:
            errors.append(f"Colonnes manquantes: {', '.join(missing_columns)}")
        
        if 'Quantité Réelle' in df.columns:
            # Conversion et validation des quantités réelles
            real_qty = pd.to_numeric(df['Quantité Réelle'], errors='coerce')
            theo_qty = pd.to_numeric(df['Quantité Théorique'], errors='coerce')
            
            # Vérification des valeurs manquantes
            missing_qty = real_qty.isna()
            if missing_qty.any():
                missing_info = df.loc[missing_qty, ['Code Article', 'Numéro Inventaire', 'Numéro Lot']].apply(
                    lambda x: f"{x['Code Article']} - Lot: {x['Numéro Lot']} (Inv: {x['Numéro Inventaire']})", axis=1
                ).tolist()
                errors.append(f"Quantités réelles manquantes pour: {', '.join(map(str, missing_info[:5]))}")
                if len(missing_info) > 5:
                    errors.append(f"... et {len(missing_info) - 5} autres articles")
            
            # Vérification des valeurs négatives
            negative_qty = real_qty < 0
            if negative_qty.any():
                negative_info = df.loc[negative_qty, ['Code Article', 'Numéro Inventaire', 'Numéro Lot']].apply(
                    lambda x: f"{x['Code Article']} - Lot: {x['Numéro Lot']} (Inv: {x['Numéro Inventaire']})", axis=1
                ).tolist()
                errors.append(f"Quantités négatives pour: {', '.join(map(str, negative_info[:5]))}")
            
            # Information sur les lots LOTECART détectés
            lotecart_mask = (theo_qty == 0) & (real_qty > 0)
            if lotecart_mask.any():
                lotecart_count = lotecart_mask.sum()
                logger.info(f"{lotecart_count} lots LOTECART détectés (quantité théorique = 0, quantité réelle > 0)")
        
        is_valid = len(errors) == 0
        message = "Template valide" if is_valid else "Erreurs détectées"
        
        return is_valid, message, errors