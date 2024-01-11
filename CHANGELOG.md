# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- [#10117: 投票データがない場合に清算処理がエラーになる](https://redmine.u6k.me/issues/10117)
- [#10123: 払戻しデータのパースがエラーになる場合がある](https://redmine.u6k.me/issues/10123)

### Added

- [#10089: プロジェクトを立ち上げる](https://redmine.u6k.me/issues/10089)
- [#10090: 舟券予測データから日次レース一覧データを作成する](https://redmine.u6k.me/issues/10090)
- [#10091: 舟券投票アクション(仮)を行う](https://redmine.u6k.me/issues/10091)
- [#10092: 投票結果清算アクション(仮)を行う](https://redmine.u6k.me/issues/10092)
- [#10097: 対象レースを探すときはレース一覧データから、実際に投票/清算するときの対象レースはローカルストレージから取得する](https://redmine.u6k.me/issues/10097)
- [#10113: 払戻はデータ読み込み時に率に直す](https://redmine.u6k.me/issues/10113)
- [#10116: 予測確率偏差値投票を実装する](https://redmine.u6k.me/issues/10116)
- [#10217: 期待値投票を再実装する](https://redmine.u6k.me/issues/10217)
- [#10255: 10,5分前オッズによる単勝、二連単の期待値投票アクションをステージング運用する](https://redmine.u6k.me/issues/10255)

### Changed

- [#10269: 期待値投票の閾値を変更する](https://redmine.u6k.me/issues/10269)
