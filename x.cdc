import FungibleToken from 0x9a0766d93b6608b7

import TopShot from 0x877931736ee77cff
import TopShotMarketV3 from 0x547f177b243b4d80

import AllDay from 0x4dfd62c88d1b6462
import PackNFT from 0x4dfd62c88d1b6462

import NFTStorefront from 0x94b06cfca1d8a476
import NFTStorefrontV2 from 0x34f3140b7f54c743

import JoyrideMultiToken from 0xe2fcb04d6481ac51
import JoyrideAccounts from 0xa5afdcc07cc7b283
import JoyridePayments from 0xa5afdcc07cc7b283
import RLY from 0xe2fcb04d6481ac51
import JRXToken from 0xe2fcb04d6481ac51
import USDCToken from 0xe627555cd932b71c
import FUSD from 0xe223d8a629e49c68
import TatumMultiNFT from 0x87fe4ebd0cddde06
import JoyrideNFTRouter from 0x2adfa46d7e8b2342
import FazeUtilityCoin from 0x34f3140b7f54c743
import CricketMoments from 0x34f3140b7f54c743

import FiatToken from 0xa983fecbed621163

pub fun main(): String {
	return FiatToken.name.concat(" ").concat(FiatToken.version)
}
