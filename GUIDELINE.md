# BNB Chain Repository Submission Guidelines — VKINHA Project

## 1. Purpose
This repository represents the **official source code of the VKINHA Token (VKx)**, deployed on the **BNB Smart Chain (BSC)**.  
It ensures accurate data tracking, ecosystem transparency, and clear alignment with the **BNB Chain ecosystem** (including BSC and other EVM-compatible networks).

---

## 2. Core Verification Principle
This repository is **compliant** because:
- Reviewers can examine the repo and **directly confirm** deployment on **BNB Smart Chain (BSC)**.  
- Smart contract code, configurations, and documentation explicitly reference **BSC** as the target chain.

---

## 3. Positive Indicators 

### Configuration Evidence
- Hardhat configuration files reference **BSC Mainnet (Chain ID: 56)** and **BSC Testnet (Chain ID: 97)**.  
- RPCs and deployment scripts point explicitly to **BSC nodes**.  

### README Documentation
- The [`README.md`](./README.md) clearly states deployment on **BNB Smart Chain**.  
- Contract verified and visible on [BscScan](https://bscscan.com/token/0xe08b716fffcc0410da0392500c6a88fe0accd819).  

### BNB Chain–Specific SDK Usage
- Uses **PancakeSwap Router V2** (`0x10ED43C718714eb63d5aA57B78B54704E256024E`), which is **native to BNB Smart Chain**.  

### Chain-Specific Files or Formats
- Deployment scripts (Hardhat) include **BSC-specific network IDs and configurations**.  

### Function Names or Signatures
- Contract code includes functions and mechanics that operate directly with **BNB-based liquidity pools**.  

### Code Comments
- Solidity comments reference **BNB Smart Chain deployment intent**.  

---

## 4. Common False Positives
This repository avoids common mis-signals:
- No misleading references to other chains as the primary deployment.  
- Explicit confirmation of **BNB Chain deployment**, not just trading of BNB.  
- Consistent references across config, code, and documentation.  

---

## 5. Submission Requirements
- ✅ Repository is **public**.  
- ✅ Contains the **official source code** of the VKINHA Token.  
- ✅ `README.md` and configs explicitly confirm deployment on **BNB Smart Chain (BSC)**.  
- ✅ This repo is the **main and official source** of the VKINHA project.  

---

### 📌 VKINHA Token — Deployment Reference
- **Contract Address (BSC Mainnet)**: [`0xe08b716fffcc0410da0392500c6a88fe0accd819`](https://bscscan.com/token/0xe08b716fffcc0410da0392500c6a88fe0accd819)  
- **Router**: PancakeSwap V2 — `0x10ED43C718714eb63d5aA57B78B54704E256024E`  
- **Total Supply**: 15,000,000 VKx  
- **Ecosystem Wallets**:  
  - Project Wallet → `0xB9A2eF80914Cb1bDBE93F04C86CBC9a54Eb0d7D2`  
  - Dev Wallet → `0x5B419e1A55e24e91D7016D4313BC5b284382Faf6`  

---
