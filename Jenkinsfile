pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                sh 'whoami'
                sh 'docker-compose build'
            }
        }

        stage('Deploy') {
            steps {
                sh 'chmod +x /var/lib/jenkins/workspace/backend/iterthesisproject/entrypoint.prod.sh'
                sh 'docker-compose -f Docker-compose.prod.yml up -d'
            }
        }
    }
}