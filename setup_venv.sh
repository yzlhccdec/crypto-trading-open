#!/bin/bash
# 虚拟环境设置脚本
# 使用方法: ./setup_venv.sh

set -e

echo "🚀 设置项目虚拟环境..."

# 检查 Python 版本
if ! command -v python3 &> /dev/null; then
    echo "❌ 错误: 未找到 python3，请先安装 Python 3.8+"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "📦 Python 版本: $(python3 --version)"

# 创建虚拟环境
if [ ! -d ".venv" ]; then
    echo "📦 创建虚拟环境 .venv..."
    python3 -m venv .venv
    echo "✅ 虚拟环境创建成功"
else
    echo "ℹ️  虚拟环境已存在"
fi

# 激活虚拟环境
echo "🔄 激活虚拟环境..."
source .venv/bin/activate

# 升级 pip
echo "⬆️  升级 pip..."
pip install --upgrade pip --quiet

# 安装依赖
if [ -f "requirements.txt" ]; then
    echo "📥 安装项目依赖..."
    
    # 先安装构建依赖（解决 pandas/numpy 编译问题）
    echo "📦 安装构建工具..."
    pip install --upgrade pip setuptools wheel --quiet
    
    # 安装依赖
    pip install -r requirements.txt || {
        echo "⚠️  部分依赖安装失败，尝试修复..."
        echo "💡 如果 pandas/numpy 安装失败，请运行: ./fix_pandas_install.sh"
    }
    echo "✅ 依赖安装完成"
else
    echo "⚠️  未找到 requirements.txt"
fi

echo ""
echo "✅ 虚拟环境设置完成！"
echo ""
echo "💡 使用说明:"
echo "   手动激活: source .venv/bin/activate"
echo "   退出环境: deactivate"
echo ""
echo "💡 自动激活 (推荐):"
echo "   1. 安装 direnv: brew install direnv"
echo "   2. 在 ~/.zshrc 或 ~/.bashrc 中添加:"
echo "      eval \"\$(direnv hook zsh)\"  # 或 bash"
echo "   3. 进入项目目录时会自动激活"


