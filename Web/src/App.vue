<script setup lang="ts">
import { ref } from 'vue'
import MultiTableJoin from './components/MultiTableJoin.vue'
import DecisionTreeViewer from './components/DecisionTreeViewer.vue'

const activeIndex = ref('1')
const handleSelect = (key: string) => {
  activeIndex.value = key
}
</script>

<template>
<div id="app">
    <div class="header">
      <div class="header-title">
        XCE
      </div>
    </div>
    <div class="main-container">
      <el-menu
        class="el-menu-demo"
        mode="horizontal"
        :default-active="activeIndex"
        @select="handleSelect"
      >
        <el-menu-item index="1">Query Plan</el-menu-item>
        <el-menu-item index="2">Decision Tree</el-menu-item>
      </el-menu>
      
      <div class="content">
        <component :is="
          activeIndex === '1' ? MultiTableJoin :
          DecisionTreeViewer
        "/>
      </div>
    </div>
</div>

</template>

<style>
#app {
  width: 100vw;
  height: 100vh;
  display: flex;
  flex-direction: column;
  background-color: #ffffff;
  color: #2c3e50;
  font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}

.monospace {
  font-family: 'JetBrains Mono', 'Fira Code', 'SF Mono', Consolas, 'Courier New', monospace;
  font-feature-settings: "liga" 1, "calt" 1;
}

.header {
  height: 60px;
  background-color: #ffffff;
  border-bottom: 1px solid #e4e7ed;
  display: flex;
  align-items: center;
  padding: 0 24px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.05);
  position: relative;
}

.header::after {
  content: '';
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  height: 2px;
  background: linear-gradient(90deg, transparent, #409EFF, transparent);
  animation: glow 2s linear infinite;
}

.header-title {
  font-family: 'SF Pro Display', -apple-system, BlinkMacSystemFont, 'Inter', sans-serif;
  font-size: 26px;
  font-weight: 600;
  color: #409EFF;
  text-shadow: none;
  letter-spacing: 0.5px;
}

.main-container {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.el-menu {
  background-color: #ffffff !important;
  border-bottom: 1px solid #e4e7ed !important;
  padding: 0 !important;
  height: 50px !important;
  margin: 0 !important;
}

.el-menu-item {
  height: 50px !important;
  line-height: 50px !important;
  color: #606266 !important;
  font-size: 15px !important;
  font-weight: 500 !important;
  padding: 0 24px !important;
  margin: 0 !important;
  letter-spacing: 0.3px !important;
  transition: all 0.3s ease !important;
}

.el-menu-item:hover {
  color: #409EFF !important;
  background-color: #f5f7fa !important;
}

.el-menu-item.is-active {
  color: #409EFF !important;
  background-color: #f5f7fa !important;
  border-bottom: 2px solid #409EFF !important;
  margin-bottom: -1px !important;
  position: relative !important;
  font-weight: 600 !important;
}

.el-menu-item.is-active::before {
  content: '';
  position: absolute;
  left: 0;
  top: 0;
  bottom: 0;
  width: 2px;
  background-color: #409EFF;
  box-shadow: none;
}

.content {
  flex: 1;
  overflow: hidden;
  padding: 20px;
  background-color: #ffffff;
}

:root {
  --el-font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
  --el-font-family-monospace: 'JetBrains Mono', 'Fira Code', 'SF Mono', Consolas, 'Courier New', monospace;
  --el-font-size-base: 14px;
  --el-font-weight-primary: 500;
  --el-bg-color: #ffffff;
  --el-bg-color-overlay: #ffffff;
  --el-text-color-primary: #2c3e50;
  --el-text-color-regular: #606266;
  --el-border-color: #e4e7ed;
  --el-border-color-light: #e4e7ed;
  --el-border-color-lighter: #ebeef5;
  --el-border-color-extra-light: #f2f6fc;
  --el-fill-color-blank: #ffffff;
  --el-fill-color-light: #f5f7fa;
  --el-fill-color-lighter: #f5f7fa;
  --el-fill-color-extra-light: #f5f7fa;
  --el-fill-color-dark: #f5f7fa;
  --el-fill-color-darker: #f5f7fa;
  --el-fill-color-blank: #ffffff;
  --el-mask-color: rgba(0, 0, 0, 0.5);
  --el-mask-color-extra-light: rgba(0, 0, 0, 0.1);
}

@keyframes glow {
  0% { opacity: 0.5; }
  50% { opacity: 1; }
  100% { opacity: 0.5; }
}
</style>
