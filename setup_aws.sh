#!/bin/bash

# Script para configurar e executar o sistema de ordenação total com Lamport

echo "=== Setup do Sistema de Ordenação Total com Lamport ==="

# Instalar dependências Python
echo "Instalando dependências..."
sudo apt-get update
sudo apt-get install -y python3 python3-pip
pip3 install requests

# Criar diretório de trabalho
mkdir -p ~/lamport_system
cd ~/lamport_system

echo "Sistema pronto para execução!"
echo ""
echo "Instruções de execução:"
echo ""
echo "1. No servidor de N. Virginia (Group Manager):"
echo "   python3 GroupMngr.py"
echo ""
echo "2. No servidor de N. Virginia (Comparison Server):"
echo "   python3 comparisonServer.py"
echo ""
echo "3. Em cada instância de peer (N. Virginia e Oregon):"
echo "   python3 peerCommunicatorUDP.py"
echo ""
echo "Certifique-se de que as constantes em constMP.py estão configuradas corretamente:"
echo "- SERVER_ADDR: IP público do Comparison Server"
echo "- GROUPMNGR_ADDR: IP público do Group Manager"
echo "- N: Número total de peers (ex: 5 para 3 em Virginia + 2 em Oregon)"