# Big Data

conda create -n bda-env python=3.10 -y
conda activate bda-env
conda install -c conda-forge openjdk=21 maven -y
pip install --upgrade pip jupyterlab kaggle pyspark

Pour Kaggle : 
Connectez-vous à votre compte sur kaggle.com.
Allez dans votre profil, puis dans la section "Account".
Cliquez sur "Create New Token". Un fichier kaggle.json sera téléchargé.
Déplacez ce fichier et sécurisez-le avec les commandes suivantes dans votre terminal :

```
mkdir -p ~/.kaggle
mv ~/Downloads/kaggle.json ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
```