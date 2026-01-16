    def update(self, asset, current_close, current_high, current_low):
        # Filter trades for this asset
        asset_trades = [t for t in self.active_trades if t['asset'] == asset]
        
        for trade in asset_trades:
            is_closed = False
            result = "OPEN"
            exit_price = current_close
            
            # Flags to track what was touched
            hit_win = False
            hit_loss = False
            
            # 1. Check Levels
            if trade['direction'] == 1: # LONG
                if current_high >= trade['target_price']: hit_win = True
                if current_low <= trade['stop_price']: hit_loss = True
            
            elif trade['direction'] == -1: # SHORT
                if current_low <= trade['target_price']: hit_win = True
                if current_high >= trade['stop_price']: hit_loss = True

            # 2. Resolve Ambiguity (The Worst-Case Rule)
            if hit_win and hit_loss:
                # Both hit in same candle -> Assume STOP hit first
                is_closed = True
                result = "LOSS_HIT"
                exit_price = trade['stop_price']
            elif hit_loss:
                is_closed = True
                result = "LOSS_HIT"
                exit_price = trade['stop_price']
            elif hit_win:
                is_closed = True
                result = "WIN_HIT"
                exit_price = trade['target_price']

            # 3. Check Expiry (if not already closed)
            if not is_closed and datetime.now() >= trade['expiry']:
                is_closed = True
                result = "EXPIRED"
                exit_price = current_close

            if is_closed:
                self._finalize_trade(trade, exit_price, result)
